package storage

import "time"

// expirationItem represents one scheduled expiration.
type expirationItem struct {
	key       string
	expiresAt time.Time
	version   uint64
	index     int
}

// expirationHeap keeps the earliest expiration at the root.
type expirationHeap []*expirationItem

func (h expirationHeap) Len() int {
	return len(h)
}

func (h expirationHeap) Less(i, j int) bool {
	if h[i].expiresAt.Equal(h[j].expiresAt) {
		return h[i].key < h[j].key
	}

	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h expirationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]

	h[i].index = i
	h[j].index = j
}

func (h *expirationHeap) Push(x any) {
	item := x.(*expirationItem)

	item.index = len(*h)
	*h = append(*h, item)
}

func (h *expirationHeap) Pop() any {
	old := *h
	n := len(old)

	item := old[n-1]

	old[n-1] = nil
	item.index = -1

	*h = old[:n-1]

	return item
}
