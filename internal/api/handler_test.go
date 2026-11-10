package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"qcache/internal/storage"
	"sync"
	"testing"
	"time"
)

func newTestHandler(t *testing.T, maxBodyBytes int64) http.Handler {
	t.Helper()

	cache := storage.New()

	t.Cleanup(func() {
		cache.Close()
	})

	return NewHandler(cache, maxBodyBytes)
}

func TestPutAndGet(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	payload := []byte(`{"name":"Piyush"}`)

	// PUT request.
	putReq := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:123",
		bytes.NewReader(payload),
	)

	putResp := httptest.NewRecorder()

	handler.ServeHTTP(putResp, putReq)

	if putResp.Code != http.StatusNoContent {
		t.Fatalf("Expected 204, got %d", putResp.Code)
	}

	// GET request.
	getReq := httptest.NewRequest(
		http.MethodGet,
		"/v1/keys/user:123",
		nil,
	)

	getResp := httptest.NewRecorder()

	handler.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", getResp.Code)
	}

	if !bytes.Equal(getResp.Body.Bytes(), payload) {
		t.Fatalf("Unexpected response body: %s", getResp.Body.Bytes())
	}
}

func TestGetMissingKey(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/keys/missing",
		nil,
	)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("Expected 404, got %d", resp.Code)
	}
}

func TestPutWithTTL(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	req := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/session:1",
		bytes.NewReader([]byte("temporary")),
	)

	req.Header.Set("X-QCache-TTL-Ms", "60000")

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("Expected 204, got %d", resp.Code)
	}

	getReq := httptest.NewRequest(
		http.MethodGet,
		"/v1/keys/session:1",
		nil,
	)

	getResp := httptest.NewRecorder()

	handler.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusOK {
		t.Fatal("Expected key to exist")
	}
}

func TestPutRejectsInvalidTTL(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	req := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:1",
		bytes.NewReader([]byte("Piyush")),
	)

	req.Header.Set("X-QCache-TTL-Ms", "0")

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("Expected 400, got %d", resp.Code)
	}
}

func TestPutRejectsOversizedPayload(t *testing.T) {

	handler := newTestHandler(t, 4)

	req := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:1",
		bytes.NewReader([]byte("12345")),
	)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("Expected 413, got %d", resp.Code)
	}
}

func TestDeleteKey(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	putReq := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:1",
		bytes.NewReader([]byte("Piyush")),
	)

	putResp := httptest.NewRecorder()

	handler.ServeHTTP(putResp, putReq)

	deleteReq := httptest.NewRequest(
		http.MethodDelete,
		"/v1/keys/user:1",
		nil,
	)

	deleteResp := httptest.NewRecorder()

	handler.ServeHTTP(deleteResp, deleteReq)

	if deleteResp.Code != http.StatusNoContent {
		t.Fatalf("Expected 204, got %d", deleteResp.Code)
	}
}

func TestHeadKey(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	putReq := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:1",
		bytes.NewReader([]byte("Piyush")),
	)

	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)

	headReq := httptest.NewRequest(
		http.MethodHead,
		"/v1/keys/user:1",
		nil,
	)

	headResp := httptest.NewRecorder()

	handler.ServeHTTP(headResp, headReq)

	if headResp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", headResp.Code)
	}

	if headResp.Body.Len() != 0 {
		t.Fatal("HEAD response should not contain a body")
	}
}

func TestHealthEndpoint(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	req := httptest.NewRequest(
		http.MethodGet,
		"/healthz",
		nil,
	)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}
}

func TestStatsEndpoint(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	putReq := httptest.NewRequest(
		http.MethodPut,
		"/v1/keys/user:1",
		bytes.NewReader([]byte("Piyush")),
	)

	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/stats",
		nil,
	)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.Code)
	}

	var result statsResponse

	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("Invalid stats JSON: %v", err)
	}

	if result.Entries != 1 {
		t.Fatalf("Expected 1 entry, got %d", result.Entries)
	}
}

func TestUnsupportedMethod(t *testing.T) {

	handler := newTestHandler(t, 1<<20)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/keys/user:1",
		nil,
	)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("Expected 405, got %d", resp.Code)
	}
}

func TestConcurrentHTTPRequests(t *testing.T) {

	cache := storage.New()
	t.Cleanup(func() {
		cache.Close()
	})

	handler := NewHandler(cache, 1<<20)

	// Configure bounded request admission.
	limiter, err := NewConcurrencyLimiter(
		8,
		128,
		5*time.Second,
	)

	if err != nil {
		t.Fatal(err)
	}

	// Start an actual HTTP server on a local TCP port.
	server := httptest.NewServer(
		limiter.Middleware(handler),
	)
	defer server.Close()

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	const totalClients = 100

	var wg sync.WaitGroup

	for i := 0; i < totalClients; i++ {

		wg.Add(1)

		go func(id int) {

			defer wg.Done()

			key := fmt.Sprintf("user:%d", id)
			expected := []byte(fmt.Sprintf("value-%d", id))

			// PUT request.
			putReq, err := http.NewRequest(
				http.MethodPut,
				server.URL+"/v1/keys/"+key,
				bytes.NewReader(expected),
			)

			if err != nil {
				t.Errorf("Creating PUT request: %v", err)
				return
			}

			putResp, err := client.Do(putReq)

			if err != nil {
				t.Errorf("PUT request failed: %v", err)
				return
			}

			putResp.Body.Close()

			if putResp.StatusCode != http.StatusNoContent {
				t.Errorf(
					"PUT: expected 204, got %d",
					putResp.StatusCode,
				)
				return
			}

			// GET request.
			getResp, err := client.Get(
				server.URL + "/v1/keys/" + key,
			)

			if err != nil {
				t.Errorf("GET request failed: %v", err)
				return
			}

			body := new(bytes.Buffer)

			_, readErr := body.ReadFrom(getResp.Body)
			getResp.Body.Close()

			if readErr != nil {
				t.Errorf("Reading GET response: %v", readErr)
				return
			}

			if getResp.StatusCode != http.StatusOK {
				t.Errorf(
					"GET: expected 200, got %d",
					getResp.StatusCode,
				)
				return
			}

			if !bytes.Equal(body.Bytes(), expected) {
				t.Errorf(
					"Unexpected value for %s",
					key,
				)
			}

		}(i)
	}

	wg.Wait()

	// Every client inserted exactly one unique key.
	if cache.Len() != totalClients {
		t.Fatalf(
			"Expected %d entries, got %d",
			totalClients,
			cache.Len(),
		)
	}
}
