package api

import (
	"errors"
	"net/http"
	"time"
)

var ErrInvalidLimiterConfig = errors.New("invalid concurrency limiter configuration")

// ConcurrencyLimiter controls how many requests
// can execute and wait for execution.
type ConcurrencyLimiter struct {
	active       chan struct{}
	queued       chan struct{}
	queueTimeout time.Duration
}

// NewConcurrencyLimiter creates a bounded request limiter.
func NewConcurrencyLimiter(
	maxInFlight int,
	maxQueued int,
	queueTimeout time.Duration,
) (*ConcurrencyLimiter, error) {

	if maxInFlight <= 0 || maxQueued < 0 || queueTimeout <= 0 {
		return nil, ErrInvalidLimiterConfig
	}

	return &ConcurrencyLimiter{
		active:       make(chan struct{}, maxInFlight),
		queued:       make(chan struct{}, maxQueued),
		queueTimeout: queueTimeout,
	}, nil
}

// Middleware wraps an HTTP handler with admission control.
func (l *ConcurrencyLimiter) Middleware(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// Don't spend resources on an already-cancelled request.
		if r.Context().Err() != nil {
			return
		}

		// Fast path: try to acquire an execution slot immediately.
		select {
		case l.active <- struct{}{}:

			defer func() {
				<-l.active
			}()

			next.ServeHTTP(w, r)
			return

		default:
			// All execution slots are occupied.
		}

		// No active slot. Try to reserve a bounded queue position.
		select {
		case l.queued <- struct{}{}:

		default:
			// The waiting queue is also full.
			l.reject(w)
			return
		}

		// Wait for an execution slot, cancellation or timeout.
		timer := time.NewTimer(l.queueTimeout)
		defer timer.Stop()

		select {

		case l.active <- struct{}{}:

			// The request is no longer waiting.
			<-l.queued

			defer func() {
				<-l.active
			}()

			// The client may have disconnected while waiting.
			if r.Context().Err() != nil {
				return
			}

			next.ServeHTTP(w, r)

		case <-r.Context().Done():

			// Remove the cancelled request from the queue.
			<-l.queued
			return

		case <-timer.C:

			// Waiting took too long.
			<-l.queued
			l.reject(w)
		}
	})
}

// reject sends a standard overload response.
func (l *ConcurrencyLimiter) reject(w http.ResponseWriter) {

	w.Header().Set("Retry-After", "1")

	writeError(
		w,
		http.StatusServiceUnavailable,
		"server is busy; retry later",
	)
}
