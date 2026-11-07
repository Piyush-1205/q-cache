package storage

import "time"

// entry represents one item stored in Q-Cache.
type entry struct {
	value     []byte
	expiresAt time.Time
}

// Checks whether an entry has expired.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() &&
		!now.Before(e.expiresAt)
}
