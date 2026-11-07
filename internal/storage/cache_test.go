package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	cache := New()

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
	cache := New()

	_, exists := cache.Get("user:999")

	if exists {
		t.Fatal("Expected key not to exist")
	}
}

func TestEmptyKey(t *testing.T) {
	cache := New()

	err := cache.Set("", []byte("Piyush"))

	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Expected ErrInvalidKey, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	cache := New()

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
	cache := New()

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
	cache := New()

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

	cache := New()

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

	cache := New()

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

	cache := New()

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

	cache := New()

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

	cache := New()

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
