package storage

import (
	"container/heap"
	"time"
)

const expirationBatchSize = 256

// Notifies the expiration worker that the heap has changed.
func (c *Cache) notifyExpirationWorker() {
	select {
	case c.wakeCh <- struct{}{}:
	default:
		// A notification is already pending.
	}
}

// Returns the earliest expiration deadline.
func (c *Cache) nextExpirationDeadline() (time.Time, bool) {

	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.expirations) == 0 {
		return time.Time{}, false
	}

	return c.expirations[0].expiresAt, true
}

// Stops a timer and drains its channel when necessary.
func stopAndDrainTimer(timer *time.Timer) {

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// Background expiration worker.
func (c *Cache) expirationLoop() {

	defer close(c.workerDone)

	timer := time.NewTimer(time.Hour)

	if !timer.Stop() {
		<-timer.C
	}

	defer timer.Stop()

	for {

		deadline, exists := c.nextExpirationDeadline()

		var timerCh <-chan time.Time

		if exists {

			delay := time.Until(deadline)

			if delay < 0 {
				delay = 0
			}

			timer.Reset(delay)

			timerCh = timer.C
		}

		select {

		case <-timerCh:
			// The next expiration deadline has arrived.
			c.expireDueBatch()

		case <-c.wakeCh:
			// A new or modified expiration was scheduled.
			stopAndDrainTimer(timer)

		case <-c.stopCh:
			// Gracefully stop the worker.
			stopAndDrainTimer(timer)
			return
		}
	}
}

// Removes at most one batch of expired entries.
// The lock is released after each batch.
func (c *Cache) expireDueBatch() {

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	processed := 0

	for processed < expirationBatchSize && len(c.expirations) > 0 {

		earliest := c.expirations[0]

		if earliest.expiresAt.After(now) {
			break
		}

		item := heap.Pop(&c.expirations).(*expirationItem)

		processed++

		// Remove the scheduled mapping if it points to this item.
		if scheduled, exists := c.scheduled[item.key]; exists &&
			scheduled == item {

			delete(c.scheduled, item.key)
		}

		current, exists := c.items[item.key]

		if !exists {
			continue
		}

		// The version check protects against stale expiration events.
		if current.version != item.version {
			continue
		}

		if current.expired(now) {
			c.deleteLocked(item.key)
		}
	}
}
