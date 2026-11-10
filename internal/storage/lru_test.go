package storage

import "testing"

func TestLRUOrdering(t *testing.T) {

	lru := newLRUList()

	user1 := lru.pushFront("user:1")
	lru.pushFront("user:2")
	lru.pushFront("user:3")

	// user:1 should be the oldest.
	if oldest := lru.oldest(); oldest.key != "user:1" {
		t.Fatalf("Expected user:1, got %s", oldest.key)
	}

	// Access user:1.
	lru.moveToFront(user1)

	// user:2 should now be the oldest.
	if oldest := lru.oldest(); oldest.key != "user:2" {
		t.Fatalf("Expected user:2, got %s", oldest.key)
	}
}

func TestLRURemove(t *testing.T) {

	lru := newLRUList()

	lru.pushFront("user:1")
	user2 := lru.pushFront("user:2")
	lru.pushFront("user:3")

	lru.remove(user2)

	if oldest := lru.oldest(); oldest.key != "user:1" {
		t.Fatalf("Expected user:1, got %s", oldest.key)
	}

	// Remove remaining entries.
	for lru.oldest() != nil {
		lru.remove(lru.oldest())
	}

	if lru.oldest() != nil {
		t.Fatal("Expected empty LRU list")
	}
}
