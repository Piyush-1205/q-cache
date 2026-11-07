package storage

import (
	"container/heap"
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
	mu sync.RWMutex

	items       map[string]entry
	expirations expirationHeap

	// Tracks the scheduled heap item for each expiring key.
	scheduled map[string]*expirationItem

	// Unique generation identifier for each update.
	nextVersion uint64

	// Background expiration worker lifecycle.
	wakeCh     chan struct{}
	stopCh     chan struct{}
	workerDone chan struct{}
	closeOnce  sync.Once
}

// New creates an initialized Cache and starts its expiration worker.
func New() *Cache {
	c := &Cache{
		items:      make(map[string]entry),
		scheduled:  make(map[string]*expirationItem),
		wakeCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		workerDone: make(chan struct{}),
	}

	go c.expirationLoop()

	return c
}

// Close stops the background expiration worker.
// Call this when the cache is no longer needed.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})

	<-c.workerDone
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

// Shared implementation of Set and SetWithTTL.
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

	// Remove any previous expiration schedule for this key.
	c.removeExpirationLocked(key)

	// Generate a new version for this update.
	c.nextVersion++

	item := entry{
		value:     valueCopy,
		expiresAt: expiresAt,
		version:   c.nextVersion,
	}

	c.items[key] = item

	// Schedule expiration only when a TTL exists.
	if !expiresAt.IsZero() {
		expiration := &expirationItem{
			key:       key,
			expiresAt: expiresAt,
			version:   item.version,
		}

		heap.Push(&c.expirations, expiration)
		c.scheduled[key] = expiration
	}

	c.mu.Unlock()

	// Wake the worker so it can recalculate its next deadline.
	c.notifyExpirationWorker()

	return nil
}

// Removes the expiration schedule for a key.
// Caller must hold the write lock.
func (c *Cache) removeExpirationLocked(key string) {

	item, exists := c.scheduled[key]

	if !exists {
		return
	}

	heap.Remove(&c.expirations, item.index)
	delete(c.scheduled, key)
}

// Deletes an entry and its expiration schedule.
// Caller must hold the write lock.
func (c *Cache) deleteLocked(key string) {
	delete(c.items, key)
	c.removeExpirationLocked(key)
}

// Get retrieves a value and lazily removes expired entries.
func (c *Cache) Get(key string) ([]byte, bool) {

	c.mu.RLock()

	item, exists := c.items[key]

	if !exists {
		c.mu.RUnlock()
		return nil, false
	}

	if !item.expired(time.Now()) {
		value := append([]byte(nil), item.value...)
		c.mu.RUnlock()

		return value, true
	}

	c.mu.RUnlock()

	// Recheck after acquiring the write lock.
	c.mu.Lock()

	item, exists = c.items[key]

	if !exists {
		c.mu.Unlock()
		return nil, false
	}

	if item.expired(time.Now()) {
		c.deleteLocked(key)
		c.mu.Unlock()

		c.notifyExpirationWorker()

		return nil, false
	}

	value := append([]byte(nil), item.value...)

	c.mu.Unlock()

	return value, true
}

// Delete removes a key.
func (c *Cache) Delete(key string) bool {

	c.mu.Lock()

	item, exists := c.items[key]

	if !exists {
		c.mu.Unlock()
		return false
	}

	wasValid := !item.expired(time.Now())

	c.deleteLocked(key)

	c.mu.Unlock()

	c.notifyExpirationWorker()

	return wasValid
}

// Exists checks whether a key exists and is unexpired.
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

	item, exists = c.items[key]

	if !exists {
		c.mu.Unlock()
		return false
	}

	if item.expired(time.Now()) {
		c.deleteLocked(key)
		c.mu.Unlock()

		c.notifyExpirationWorker()

		return false
	}

	c.mu.Unlock()

	return true
}

// Len returns the number of resident entries.
// Expired entries may briefly remain until cleanup occurs.
func (c *Cache) Len() int {

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}
