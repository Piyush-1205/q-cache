package storage

// lruNode represents one node in the LRU linked list.
type lruNode struct {
	key  string
	prev *lruNode
	next *lruNode
}

// lruList maintains entries in recency order.
type lruList struct {
	head *lruNode
	tail *lruNode
}

// Creates an empty LRU list.
func newLRUList() lruList {

	head := &lruNode{}
	tail := &lruNode{}

	head.next = tail
	tail.prev = head

	return lruList{
		head: head,
		tail: tail,
	}
}

// Inserts a new node at the front.
func (l *lruList) pushFront(key string) *lruNode {

	node := &lruNode{
		key: key,
	}

	node.prev = l.head
	node.next = l.head.next

	l.head.next.prev = node
	l.head.next = node

	return node
}

// Removes a node from the list.
func (l *lruList) remove(node *lruNode) {

	if node == nil || node.prev == nil || node.next == nil {
		return
	}

	node.prev.next = node.next
	node.next.prev = node.prev

	node.prev = nil
	node.next = nil
}

// Moves an existing node to the front.
func (l *lruList) moveToFront(node *lruNode) {

	if node == nil || l.head.next == node {
		return
	}

	l.remove(node)

	node.prev = l.head
	node.next = l.head.next

	l.head.next.prev = node
	l.head.next = node
}

// Returns the least recently used node.
func (l *lruList) oldest() *lruNode {

	if l.tail.prev == l.head {
		return nil
	}

	return l.tail.prev
}
