package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"

	"qcache/internal/api"
	"qcache/internal/storage"
)

func TestHTTP3AndHTTP1ShareCache(t *testing.T) {
	// Create the actual Q-Cache storage engine.
	cache, err := storage.NewWithOptions(
		storage.Options{
			MaxMemoryBytes: 1 << 20,
		},
	)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	t.Cleanup(cache.Close)

	// Both protocols will use this same API handler.
	handler := api.NewHandler(cache, 1<<20)

	// Start a temporary HTTP/1.1 server.
	http1Server := httptest.NewServer(handler)
	t.Cleanup(http1Server.Close)

	// Generate a temporary certificate for testing.
	certFile, keyFile, roots := createTestCertificate(t)

	// Configure the HTTP/3 server.
	srv := New("127.0.0.1:0", handler)

	if err := srv.ConfigureHTTP3(
		"127.0.0.1:0",
		certFile,
		keyFile,
	); err != nil {
		t.Fatalf("configure HTTP/3: %v", err)
	}

	// Verify that QUIC configuration exists.
	if srv.http3Server.QUICConfig == nil {
		t.Fatal("QUIC configuration is missing")
	}

	// Verify that 0-RTT is disabled.
	if srv.http3Server.QUICConfig.Allow0RTT {
		t.Fatal("0-RTT must be disabled")
	}

	// Start the HTTP/3 server.
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- srv.ListenAndServeHTTP3()
	}()

	// Get the UDP port assigned by the operating system.
	http3Addr := srv.http3PacketConn.LocalAddr().String()
	http3URL := "https://" + http3Addr

	// Create an HTTP/3 client that trusts our test certificate.
	transport := &http3.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS13,
			NextProtos: []string{http3.NextProtoH3},
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	// Shut down the client and server after testing.
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Errorf("close HTTP/3 client: %v", err)
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown server: %v", err)
		}

		select {
		case err := <-serverErr:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("HTTP/3 server stopped unexpectedly: %v", err)
			}

		case <-time.After(2 * time.Second):
			t.Error("HTTP/3 server did not stop")
		}
	})

	// Wait until the HTTP/3 server is ready.
	waitForHTTP3(t, client, http3URL+"/healthz")

	// Test 1: Store through HTTP/3, retrieve through HTTP/1.1.
	t.Run("HTTP3 write and HTTP1 read", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodPut,
			http3URL+"/v1/keys/from-h3",
			strings.NewReader("stored via HTTP/3"),
		)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("HTTP/3 PUT: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected HTTP 204, got %s", resp.Status)
		}

		if resp.ProtoMajor != 3 {
			t.Fatalf("expected HTTP/3, got %s", resp.Proto)
		}

		resp, err = http1Server.Client().Get(
			http1Server.URL + "/v1/keys/from-h3",
		)
		if err != nil {
			t.Fatalf("HTTP/1.1 GET: %v", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected HTTP 200, got %s", resp.Status)
		}

		if string(body) != "stored via HTTP/3" {
			t.Fatalf("unexpected value: %q", body)
		}

		if resp.ProtoMajor != 1 {
			t.Fatalf("expected HTTP/1.1, got %s", resp.Proto)
		}
	})

	// Test 2: Store through HTTP/1.1, retrieve through HTTP/3.
	t.Run("HTTP1 write and HTTP3 read", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodPut,
			http1Server.URL+"/v1/keys/from-h1",
			strings.NewReader("stored via HTTP/1.1"),
		)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http1Server.Client().Do(req)
		if err != nil {
			t.Fatalf("HTTP/1.1 PUT: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected HTTP 204, got %s", resp.Status)
		}

		resp, err = client.Get(
			http3URL + "/v1/keys/from-h1",
		)
		if err != nil {
			t.Fatalf("HTTP/3 GET: %v", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected HTTP 200, got %s", resp.Status)
		}

		if string(body) != "stored via HTTP/1.1" {
			t.Fatalf("unexpected value: %q", body)
		}

		if resp.ProtoMajor != 3 {
			t.Fatalf("expected HTTP/3, got %s", resp.Proto)
		}
	})
}

// Generate a temporary self-signed certificate for testing.
func createTestCertificate(
	t *testing.T,
) (string, string, *x509.CertPool) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	template := x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),

		Subject: pkix.Name{
			CommonName: "localhost",
		},

		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		KeyUsage: x509.KeyUsageDigitalSignature |
			x509.KeyUsageCertSign,

		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},

		BasicConstraintsValid: true,
		IsCA:                  true,

		DNSNames: []string{"localhost"},

		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
		},
	}

	certDER, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatal(err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyDER,
	})

	dir := t.TempDir()

	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	roots := x509.NewCertPool()

	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to add test certificate to root pool")
	}

	return certFile, keyFile, roots
}

// Wait until the HTTP/3 server is ready.
func waitForHTTP3(
	t *testing.T,
	client *http.Client,
	url string,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	var lastErr error

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)

		if err == nil {
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("health check returned %s", resp.Status)
			}

			return
		}

		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("HTTP/3 server did not become ready: %v", lastErr)
}
