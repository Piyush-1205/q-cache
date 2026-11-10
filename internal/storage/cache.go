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

	items map[string]entry
	lru   lruList

	expirations expirationHeap
	scheduled   map[string]*expirationItem

	nextVersion uint64

	wakeCh     chan struct{}
	stopCh     chan struct{}
	workerDone chan struct{}
	closeOnce  sync.Once

	usedMemoryBytes    int64
	maxMemoryBytes     int64
	entryOverheadBytes int64
}

// New creates an initialized Cache.
// New creates a cache with unlimited accounted memory.
func New() *Cache {
	cache, err := NewWithOptions(Options{})

	if err != nil {
		panic(err)
	}

	return cache
}

// NewWithOptions creates a cache with configurable memory limits.
func NewWithOptions(opts Options) (*Cache, error) {

	if opts.MaxMemoryBytes < 0 ||
		opts.EntryOverheadBytes < 0 {
		return nil, ErrInvalidMemoryOptions
	}

	overhead := opts.EntryOverheadBytes

	if overhead == 0 {
		overhead = defaultEntryOverheadBytes
	}

	c := &Cache{
		items:     make(map[string]entry),
		lru:       newLRUList(),
		scheduled: make(map[string]*expirationItem),

		maxMemoryBytes:     opts.MaxMemoryBytes,
		entryOverheadBytes: overhead,

		wakeCh:     make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		workerDone: make(chan struct{}),
	}

	go c.expirationLoop()

	return c, nil
}

// Close gracefully stops the expiration worker.
func (c *Cache) Close() {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})

	<-c.workerDone
}

// Set inserts or updates a permanent entry.
func (c *Cache) Set(key string, value []byte) error {
	return c.set(key, value, time.Time{})
}

// SetWithTTL inserts or updates an expiring entry.
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

	// Calculate estimated memory cost.
	entryCost := int64(len(key)) +
		int64(len(value)) +
		c.entryOverheadBytes

	// Reject an entry that cannot fit even in an empty cache.
	if c.maxMemoryBytes > 0 &&
		entryCost > c.maxMemoryBytes {

		return ErrEntryTooLarge
	}

	// Copy input data to protect internal storage.
	valueCopy := append([]byte(nil), value...)

	c.mu.Lock()

	// Remove previous version if this key already exists.
	if _, exists := c.items[key]; exists {
		c.deleteLocked(key)
	}

	// Make sufficient room before inserting.
	if err := c.evictUntilFitsLocked(entryCost); err != nil {
		c.mu.Unlock()
		return err
	}

	// Generate a new version identifier.
	c.nextVersion++

	// Insert as most recently used.
	node := c.lru.pushFront(key)

	item := entry{
		value:      valueCopy,
		expiresAt:  expiresAt,
		version:    c.nextVersion,
		lruNode:    node,
		memoryCost: entryCost,
	}

	c.items[key] = item

	// Update memory accounting.
	c.usedMemoryBytes += entryCost

	// Schedule expiration if necessary.
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

	// Notify expiration worker.
	c.notifyExpirationWorker()

	return nil
}

// Removes an expiration schedule.
// Caller must hold the write lock.
func (c *Cache) removeExpirationLocked(key string) {

	item, exists := c.scheduled[key]

	if !exists {
		return
	}

	heap.Remove(&c.expirations, item.index)
	delete(c.scheduled, key)
}

// deleteLocked removes an entry from all storage structures.
// Caller must hold the write lock.
func (c *Cache) deleteLocked(key string) {

	if item, exists := c.items[key]; exists {

		// Remove from LRU.
		c.lru.remove(item.lruNode)

		// Release accounted memory.
		c.usedMemoryBytes -= item.memoryCost

		// Remove from hash map.
		delete(c.items, key)
	}

	// Remove any scheduled expiration.
	c.removeExpirationLocked(key)
}

// evictUntilFitsLocked removes least recently used entries
// until the requested memory allocation can fit.
//
// Caller must hold the write lock.
func (c *Cache) evictUntilFitsLocked(required int64) error {

	// Zero means unlimited memory accounting.
	if c.maxMemoryBytes == 0 {
		return nil
	}

	for c.usedMemoryBytes > c.maxMemoryBytes-required {

		// Find the least recently used entry.
		oldest := c.lru.oldest()

		if oldest == nil {
			return ErrMemoryAccounting
		}

		// Remove it from every storage structure.
		c.deleteLocked(oldest.key)
	}

	return nil
}

// Get retrieves a value and updates its LRU position.
func (c *Cache) Get(key string) ([]byte, bool) {

	// LRU access modifies the linked list.
	// Therefore, we need an exclusive lock.
	c.mu.Lock()

	item, exists := c.items[key]

	if !exists {
		c.mu.Unlock()
		return nil, false
	}

	// Lazy expiration.
	if item.expired(time.Now()) {

		c.deleteLocked(key)

		c.mu.Unlock()

		c.notifyExpirationWorker()

		return nil, false
	}

	// Mark this key as most recently used.
	c.lru.moveToFront(item.lruNode)

	// Protect internal storage from external modification.
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

// Exists checks whether a key exists and updates its recency.
func (c *Cache) Exists(key string) bool {

	c.mu.Lock()

	item, exists := c.items[key]

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

	// Successful existence checks count as accesses.
	c.lru.moveToFront(item.lruNode)

	c.mu.Unlock()

	return true
}

// Len returns the number of resident entries.
func (c *Cache) Len() int {

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items)
}
