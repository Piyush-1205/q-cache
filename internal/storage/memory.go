package storage

import "errors"

const defaultEntryOverheadBytes int64 = 64

var (
	ErrEntryTooLarge = errors.New(
		"entry exceeds maximum cache memory",
	)

	ErrInvalidMemoryOptions = errors.New(
		"memory limits cannot be negative",
	)

	ErrMemoryAccounting = errors.New(
		"memory accounting invariant violated",
	)
)

// Options configures the storage engine.
type Options struct {
	// Maximum accounted memory in bytes.
	// Zero means unlimited.
	MaxMemoryBytes int64

	// Estimated metadata allowance per entry.
	// Zero selects the default of 64 bytes.
	EntryOverheadBytes int64
}

// MemoryUsage returns the current accounted memory.
func (c *Cache) MemoryUsage() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.usedMemoryBytes
}

// MaxMemory returns the configured memory limit.
// Zero means unlimited.
func (c *Cache) MaxMemory() int64 {
	return c.maxMemoryBytes
}
