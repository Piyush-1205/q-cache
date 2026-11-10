package api

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiterRejectsWhenQueueIsFull(t *testing.T) {

	limiter, err := NewConcurrencyLimiter(
		1,
		0,
		100*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	firstDone := make(chan int, 1)

	baseHandler := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		entered <- struct{}{}

		// Hold the first request until the test releases it.
		<-release

		w.WriteHeader(http.StatusOK)
	})

	handler := limiter.Middleware(baseHandler)

	// Start the first request.
	go func() {

		req := httptest.NewRequest(
			http.MethodGet,
			"/",
			nil,
		)

		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		firstDone <- resp.Code
	}()

	// Wait until the first request occupies the active slot.
	<-entered

	// Try another request while capacity is full.
	secondReq := httptest.NewRequest(
		http.MethodGet,
		"/",
		nil,
	)

	secondResp := httptest.NewRecorder()

	handler.ServeHTTP(secondResp, secondReq)

	// Release the first request.
	close(release)

	firstStatus := <-firstDone

	if firstStatus != http.StatusOK {
		t.Fatalf("Expected first request 200, got %d", firstStatus)
	}

	if secondResp.Code != http.StatusServiceUnavailable {
		t.Fatalf("Expected second request 503, got %d", secondResp.Code)
	}

	if secondResp.Header().Get("Retry-After") == "" {
		t.Fatal("Expected Retry-After header")
	}
}

func TestLimiterQueueTimeout(t *testing.T) {

	limiter, err := NewConcurrencyLimiter(
		1,
		1,
		30*time.Millisecond,
	)

	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	firstDone := make(chan struct{})

	baseHandler := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})

	handler := limiter.Middleware(baseHandler)

	// Occupy the only active slot.
	go func() {

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		close(firstDone)
	}()

	<-entered

	// This request gets a waiting position but times out.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	close(release)
	<-firstDone

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("Expected 503, got %d", resp.Code)
	}
}

func TestLimiterCapsActiveRequests(t *testing.T) {

	const maxActive = 4
	const totalRequests = 24

	limiter, err := NewConcurrencyLimiter(
		maxActive,
		32,
		2*time.Second,
	)

	if err != nil {
		t.Fatal(err)
	}

	var active atomic.Int32
	var maximum atomic.Int32

	baseHandler := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {

		current := active.Add(1)
		defer active.Add(-1)

		// Record the highest number of simultaneous handlers.
		for {
			previous := maximum.Load()

			if current <= previous {
				break
			}

			if maximum.CompareAndSwap(previous, current) {
				break
			}
		}

		// Simulate a handler doing some work.
		time.Sleep(10 * time.Millisecond)

		w.WriteHeader(http.StatusOK)
	})

	handler := limiter.Middleware(baseHandler)

	var wg sync.WaitGroup
	statuses := make(chan int, totalRequests)

	for i := 0; i < totalRequests; i++ {

		wg.Add(1)

		go func() {
			defer wg.Done()

			req := httptest.NewRequest(
				http.MethodGet,
				"/",
				nil,
			)

			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)

			statuses <- resp.Code
		}()
	}

	wg.Wait()
	close(statuses)

	for status := range statuses {
		if status != http.StatusOK {
			t.Errorf("Expected 200, got %d", status)
		}
	}

	if maximum.Load() > maxActive {
		t.Fatalf(
			"Concurrency limit violated: maximum active = %d",
			maximum.Load(),
		)
	}

	t.Logf(
		"Maximum simultaneous handlers: %d",
		maximum.Load(),
	)
}
