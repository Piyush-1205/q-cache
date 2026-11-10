package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestCache(t *testing.T) *Cache {
	t.Helper()

	cache := New()

	t.Cleanup(func() {
		cache.Close()
	})

	return cache
}
func TestSetAndGet(t *testing.T) {
	cache := newTestCache(t)

	err := cache.Set("user:1", []byte("Piyush"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, exists := cache.Get("user:1")

	if !exists {
		t.Fatal("Expected key to exist")
	}

	if string(value) != "Piyush" {
		t.Fatalf("Expected Piyush, got %s", value)
	}
}

func TestMissingKey(t *testing.T) {
	cache := newTestCache(t)

	_, exists := cache.Get("user:999")

	if exists {
		t.Fatal("Expected key not to exist")
	}
}

func TestEmptyKey(t *testing.T) {
	cache := newTestCache(t)

	err := cache.Set("", []byte("Piyush"))

	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	cache := newTestCache(t)

	cache.Set("user:1", []byte("Piyush"))

	deleted := cache.Delete("user:1")

	if !deleted {
		t.Fatal("Expected key to be deleted")
	}

	if cache.Exists("user:1") {
		t.Fatal("Key still exists after deletion")
	}
}

func TestValuesAreCopied(t *testing.T) {
	cache := newTestCache(t)

	original := []byte("Piyush")

	cache.Set("user:1", original)

	original[0] = 'R'

	value, _ := cache.Get("user:1")

	if string(value) != "Piyush" {
		t.Fatal("Original slice modified stored data")
	}

	value[0] = 'X'

	value, _ = cache.Get("user:1")

	if string(value) != "Piyush" {
		t.Fatal("Returned slice modified stored data")
	}
}

func TestConcurrentAccess(t *testing.T) {
	cache := newTestCache(t)

	var wg sync.WaitGroup

	const workers = 1000

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			key := fmt.Sprintf("user:%d", id)

			if err := cache.Set(key, []byte("Piyush")); err != nil {
				t.Errorf("Set failed: %v", err)
				return
			}

			_, exists := cache.Get(key)

			if !exists {
				t.Errorf("Key %s not found", key)
			}
		}(i)
	}

	wg.Wait()

	if cache.Len() != workers {
		t.Fatalf("Expected %d entries, got %d", workers, cache.Len())
	}
}

func TestSetWithTTL(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Piyush"),
		time.Second,
	)

	if err != nil {
		t.Fatalf("SetWithTTL failed: %v", err)
	}

	value, exists := cache.Get("session:1")

	if !exists {
		t.Fatal("Expected key to exist")
	}

	if string(value) != "Piyush" {
		t.Fatalf("Expected Piyush, got %s", value)
	}
}

func TestExpiredKey(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Piyush"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	_, exists := cache.Get("session:1")

	if exists {
		t.Fatal("Expected expired key to return a cache miss")
	}

	if cache.Len() != 0 {
		t.Fatal("Expected expired entry to be removed")
	}
}

func TestExistsForExpiredKey(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Piyush"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if cache.Exists("session:1") {
		t.Fatal("Expired key should not exist")
	}
}

func TestSetReplacesPreviousTTL(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Old"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	// Replace the expiring entry with a permanent entry.
	err = cache.Set("session:1", []byte("New"))

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	value, exists := cache.Get("session:1")

	if !exists {
		t.Fatal("Permanent replacement should not expire")
	}

	if string(value) != "New" {
		t.Fatalf("Expected New, got %s", value)
	}
}

func TestInvalidTTL(t *testing.T) {

	cache := newTestCache(t)

	invalidTTLs := []time.Duration{
		0,
		-time.Second,
	}

	for _, ttl := range invalidTTLs {

		err := cache.SetWithTTL(
			"session:1",
			[]byte("Piyush"),
			ttl,
		)

		if !errors.Is(err, ErrInvalidTTL) {
			t.Errorf(
				"Expected ErrInvalidTTL for TTL %v, got %v",
				ttl,
				err,
			)
		}
	}
}

func TestActiveExpirationRemovesUnaccessedKey(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Piyush"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)

	// Do not call Get or Exists here.
	// Only check whether the background worker removed the entry.
	for cache.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if cache.Len() != 0 {
		t.Fatal("Background worker did not remove expired entry")
	}

	if cache.lru.oldest() != nil {
		t.Fatal("Expired entry was not removed from LRU")
	}
}

func TestUpdatedTTLDoesNotExpireEarly(t *testing.T) {

	cache := newTestCache(t)

	err := cache.SetWithTTL(
		"session:1",
		[]byte("Old"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)

	// Replace with a new value and longer TTL.
	err = cache.SetWithTTL(
		"session:1",
		[]byte("New"),
		500*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	value, exists := cache.Get("session:1")

	if !exists {
		t.Fatal("Updated key was incorrectly expired")
	}

	if string(value) != "New" {
		t.Fatalf("Expected New, got %s", value)
	}
}

func TestGetUpdatesLRURecency(t *testing.T) {

	cache := newTestCache(t)

	cache.Set("user:1", []byte("Piyush"))
	cache.Set("user:2", []byte("Rahul"))
	cache.Set("user:3", []byte("Aman"))

	// user:1 was inserted first, so it is currently oldest.
	if oldest := cache.lru.oldest(); oldest.key != "user:1" {
		t.Fatalf("Expected user:1, got %s", oldest.key)
	}

	// Access user:1.
	cache.Get("user:1")

	// user:2 should now be oldest.
	if oldest := cache.lru.oldest(); oldest.key != "user:2" {
		t.Fatalf("Expected user:2, got %s", oldest.key)
	}
}

func TestDeleteRemovesLRUNode(t *testing.T) {

	cache := newTestCache(t)

	cache.Set("user:1", []byte("Piyush"))
	cache.Set("user:2", []byte("Rahul"))

	cache.Delete("user:1")

	if oldest := cache.lru.oldest(); oldest == nil {
		t.Fatal("Expected user:2 to remain")
	} else if oldest.key != "user:2" {
		t.Fatalf("Expected user:2, got %s", oldest.key)
	}

	if cache.Exists("user:1") {
		t.Fatal("Deleted key still exists")
	}
}

func TestSetReplacementDoesNotDuplicateLRUNode(t *testing.T) {

	cache := newTestCache(t)

	cache.Set("user:1", []byte("Old"))
	cache.Set("user:2", []byte("Rahul"))

	cache.Set("user:1", []byte("New"))

	count := 0

	for node := cache.lru.head.next; node != cache.lru.tail; node = node.next {

		if node.key == "user:1" {
			count++
		}
	}

	if count != 1 {
		t.Fatalf("Expected one user:1 node, found %d", count)
	}

	value, exists := cache.Get("user:1")

	if !exists || string(value) != "New" {
		t.Fatal("Updated value was not stored correctly")
	}
}

func newTestCacheWithOptions(
	t *testing.T,
	opts Options,
) *Cache {

	t.Helper()

	cache, err := NewWithOptions(opts)

	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	t.Cleanup(func() {
		cache.Close()
	})

	return cache
}

func TestMemoryAccountingOnSet(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 1024},
	)

	err := cache.Set("a", []byte("12345"))

	if err != nil {
		t.Fatal(err)
	}

	expected := int64(1 + 5 + defaultEntryOverheadBytes)

	if got := cache.MemoryUsage(); got != expected {
		t.Fatalf("Expected %d bytes, got %d", expected, got)
	}
}

func TestMemoryAccountingOnUpdate(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 1024},
	)

	cache.Set("a", []byte("12345"))

	cache.Set("a", []byte("1234567890"))

	expected := int64(1 + 10 + defaultEntryOverheadBytes)

	if got := cache.MemoryUsage(); got != expected {
		t.Fatalf("Expected %d bytes, got %d", expected, got)
	}

	if cache.Len() != 1 {
		t.Fatal("Updating a key should not create another entry")
	}
}

func TestMemoryAccountingOnDelete(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 1024},
	)

	cache.Set("a", []byte("12345"))

	cache.Delete("a")

	if got := cache.MemoryUsage(); got != 0 {
		t.Fatalf("Expected 0 bytes, got %d", got)
	}
}

func TestLRUEviction(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 150},
	)

	// Each entry costs 75 bytes:
	// 1 byte key + 10 byte value + 64 byte overhead.

	cache.Set("a", []byte("1234567890"))
	cache.Set("b", []byte("1234567890"))

	// Access a, making it recently used.
	cache.Get("a")

	// Inserting c requires evicting one entry.
	err := cache.Set("c", []byte("1234567890"))

	if err != nil {
		t.Fatal(err)
	}

	if !cache.Exists("a") {
		t.Fatal("Recently used key a should remain")
	}

	if cache.Exists("b") {
		t.Fatal("Least recently used key b should be evicted")
	}

	if !cache.Exists("c") {
		t.Fatal("Newly inserted key c should exist")
	}

	if cache.Len() != 2 {
		t.Fatalf("Expected 2 entries, got %d", cache.Len())
	}

	if cache.MemoryUsage() != 150 {
		t.Fatalf("Expected 150 bytes, got %d", cache.MemoryUsage())
	}
}

func TestEntryExceedingMemoryLimit(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 70},
	)

	// Cost = 1 + 6 + 64 = 71 bytes.
	err := cache.Set("a", []byte("123456"))

	if !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Expected ErrEntryTooLarge, got %v", err)
	}

	if cache.Len() != 0 {
		t.Fatal("Oversized entry should not be inserted")
	}

	if cache.MemoryUsage() != 0 {
		t.Fatal("Rejected entry should not consume accounted memory")
	}
}

func TestExpirationReleasesMemory(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 1024},
	)

	err := cache.SetWithTTL(
		"a",
		[]byte("12345"),
		50*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)

	for cache.MemoryUsage() != 0 &&
		time.Now().Before(deadline) {

		time.Sleep(5 * time.Millisecond)
	}

	if cache.MemoryUsage() != 0 {
		t.Fatal("Expired entry did not release accounted memory")
	}
}

func TestEvictionRemovesExpirationSchedule(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 150},
	)

	err := cache.SetWithTTL(
		"a",
		[]byte("1234567890"),
		time.Hour,
	)

	if err != nil {
		t.Fatal(err)
	}

	cache.Set("b", []byte("1234567890"))

	// Make b recently used, leaving a as the LRU entry.
	cache.Get("b")

	// This insertion should evict a.
	err = cache.Set("c", []byte("1234567890"))

	if err != nil {
		t.Fatal(err)
	}

	cache.mu.RLock()
	_, scheduled := cache.scheduled["a"]
	heapLength := len(cache.expirations)
	cache.mu.RUnlock()

	if scheduled {
		t.Fatal("Evicted key still has an expiration schedule")
	}

	if heapLength != 0 {
		t.Fatalf("Expected empty expiration heap, got %d records", heapLength)
	}
}

func TestOversizedReplacementPreservesOldValue(t *testing.T) {

	cache := newTestCacheWithOptions(
		t,
		Options{MaxMemoryBytes: 80},
	)

	err := cache.Set("a", []byte("valid"))

	if err != nil {
		t.Fatal(err)
	}

	// This replacement exceeds the entire budget.
	err = cache.Set("a", []byte("this value is too large"))

	if !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("Expected ErrEntryTooLarge, got %v", err)
	}

	value, exists := cache.Get("a")

	if !exists || string(value) != "valid" {
		t.Fatal("Failed replacement should preserve the old value")
	}
}

func TestInvalidMemoryOptions(t *testing.T) {

	_, err := NewWithOptions(
		Options{
			MaxMemoryBytes: -1,
		},
	)

	if !errors.Is(err, ErrInvalidMemoryOptions) {
		t.Fatalf("Expected ErrInvalidMemoryOptions, got %v", err)
	}
}
