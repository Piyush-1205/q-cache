package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

// The API depends on storage behavior,
// not on a particular storage implementation.
type cacheStore interface {
	Set(key string, value []byte) error
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	Get(key string) ([]byte, bool)
	Delete(key string) bool
	Exists(key string) bool
	Len() int
	MemoryUsage() int64
	MaxMemory() int64
}

const defaultMaxBodyBytes int64 = 1 << 20 // 1 MiB

type Handler struct {
	cache        cacheStore
	maxBodyBytes int64
}

type errorResponse struct {
	Error string `json:"error"`
}

type statsResponse struct {
	Entries         int   `json:"entries"`
	UsedMemoryBytes int64 `json:"used_memory_bytes"`
	MaxMemoryBytes  int64 `json:"max_memory_bytes"`
}

// NewHandler creates the HTTP API handler.
func NewHandler(cache cacheStore, maxBodyBytes int64) http.Handler {

	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultMaxBodyBytes
	}

	h := &Handler{
		cache:        cache,
		maxBodyBytes: maxBodyBytes,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /v1/stats", h.stats)

	mux.HandleFunc("PUT /v1/keys/{key}", h.putKey)
	mux.HandleFunc("GET /v1/keys/{key}", h.getKey)
	mux.HandleFunc("HEAD /v1/keys/{key}", h.headKey)
	mux.HandleFunc("DELETE /v1/keys/{key}", h.deleteKey)

	return mux
}

// Writes a consistent JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(errorResponse{
		Error: message,
	})
}

// PUT /v1/keys/{key}
func (h *Handler) putKey(w http.ResponseWriter, r *http.Request) {

	key := r.PathValue("key")

	if key == "" {
		writeError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	// Restrict request body size to protect server memory.
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	value, err := io.ReadAll(r.Body)

	if err != nil {

		var maxBytesErr *http.MaxBytesError

		if errors.As(err, &maxBytesErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload too large")
			return
		}

		writeError(w, http.StatusBadRequest, "unable to read request body")
		return
	}

	ttlHeader := r.Header.Get("X-QCache-TTL-Ms")

	if ttlHeader == "" {

		err = h.cache.Set(key, value)

	} else {

		ttlMillis, parseErr := strconv.ParseInt(ttlHeader, 10, 64)

		if parseErr != nil || ttlMillis <= 0 {
			writeError(w, http.StatusBadRequest, "TTL must be a positive integer in milliseconds")
			return
		}

		maxTTLMillis := int64((time.Duration(1<<63 - 1)) / time.Millisecond)

		if ttlMillis > maxTTLMillis {
			writeError(w, http.StatusBadRequest, "TTL is too large")
			return
		}

		err = h.cache.SetWithTTL(
			key,
			value,
			time.Duration(ttlMillis)*time.Millisecond,
		)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to store value")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /v1/keys/{key}
func (h *Handler) getKey(w http.ResponseWriter, r *http.Request) {

	key := r.PathValue("key")

	value, exists := h.cache.Get(key)

	if !exists {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(value)
}

// HEAD /v1/keys/{key}
func (h *Handler) headKey(w http.ResponseWriter, r *http.Request) {

	key := r.PathValue("key")

	if !h.cache.Exists(key) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DELETE /v1/keys/{key}
func (h *Handler) deleteKey(w http.ResponseWriter, r *http.Request) {

	key := r.PathValue("key")

	deleted := h.cache.Delete(key)

	if !deleted {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /healthz
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	_, _ = io.WriteString(w, "ok\n")
}

// GET /v1/stats
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	response := statsResponse{
		Entries:         h.cache.Len(),
		UsedMemoryBytes: h.cache.MemoryUsage(),
		MaxMemoryBytes:  h.cache.MaxMemory(),
	}

	_ = json.NewEncoder(w).Encode(response)
}
