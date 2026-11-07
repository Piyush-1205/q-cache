package storage

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidKey = errors.New("key cannot be empty")
	ErrInvalidTTL = errors.New("TTL must be greater than zero")
)

// Cache is Q-Cache's in-memory storage engine.
type Cache struct {
	mu    sync.RWMutex
	items map[string]entry
}

// New creates an initialized Cache.
func New() *Cache {
	return &Cache{
		items: make(map[string]entry),
	}
}

// Set inserts or updates a key without expiration.
func (c *Cache) Set(key string, value []byte) error {
	return c.set(key, value, time.Time{})
}

// SetWithTTL inserts or updates a key with an expiration.
func (c *Cache) SetWithTTL(
	key string,
	value []byte,
	ttl time.Duration,
) error {

	if ttl <= 0 {
		return ErrInvalidTTL
	}

	expiresAt := time.Now().Add(ttl)

	return c.set(key, value, expiresAt)
}

// Internal method shared by Set and SetWithTTL.
func (c *Cache) set(
	key string,
	value []byte,
	expiresAt time.Time,
) error {

	if key == "" {
		return ErrInvalidKey
	}

	valueCopy := append([]byte(nil), value...)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = entry{
		value:     valueCopy,
		expiresAt: expiresAt,
	}

	return nil
}

// Get retrieves a value and lazily removes expired entries.
func (c *Cache) Get(key string) ([]byte, bool) {

	c.mu.RLock()

	item, exists := c.items[key]

	if !exists {
		c.mu.RUnlock()
		return nil, false
	}

	// Fast path: the entry has not expired.
	if !item.expired(time.Now()) {
		result := append([]byte(nil), item.value...)
		c.mu.RUnlock()
		return result, true
	}

	c.mu.RUnlock()

	// Expired entry: acquire write lock to remove it.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Recheck because another goroutine may have updated it.
	item, exists = c.items[key]

	if !exists {
		return nil, false
	}

	if item.expired(time.Now()) {
		delete(c.items, key)
		return nil, false
	}

	result := append([]byte(nil), item.value...)

	return result, true
}

// Delete removes a key.
func (c *Cache) Delete(key string) bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]

	if !exists {
		return false
	}

	delete(c.items, key)

	// An expired key is logically absent.
	return !item.expired(time.Now())
}

// Exists checks whether a key is present and unexpired.
func (c *Cache) Exists(key string) bool {

	c.mu.RLock()

	item, exists := c.items[key]

	if !exists {
		c.mu.RUnlock()
		return false
	}

	if !item.expired(time.Now()) {
		c.mu.RUnlock()
		return true
	}

	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists = c.items[key]

	if !exists {
		return false
	}

	if item.expired(time.Now()) {
		delete(c.items, key)
		return false
	}

	return true
}

// Len returns the number of resident entries.
func (c *Cache) Len() int {

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}
