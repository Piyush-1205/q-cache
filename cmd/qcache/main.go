package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qcache/internal/api"
	"qcache/internal/server"
	"qcache/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {

	// Initialize the storage engine.
	cache, err := storage.NewWithOptions(
		storage.Options{
			MaxMemoryBytes: 512 << 20, // 512 MiB
		},
	)

	if err != nil {
		return fmt.Errorf("initialize cache: %w", err)
	}

	defer cache.Close()

	// Create the HTTP API.
	handler := api.NewHandler(cache, 1<<20)

	// Configure request admission control.
	limiter, err := api.NewConcurrencyLimiter(
		64,
		128,
		100*time.Millisecond,
	)

	if err != nil {
		return fmt.Errorf("configure concurrency limiter: %w", err)
	}

	// Wrap all HTTP endpoints with the limiter.
	handler = limiter.Middleware(handler)

	// Configure both servers.
	const (
		http1Addr = "127.0.0.1:8080"
		http3Addr = "127.0.0.1:8443"

		certFile = "dev-certs/server.crt"
		keyFile  = "dev-certs/server.key"
	)

	srv := server.New(http1Addr, handler)

	// Prepare the HTTP/3 server.
	if err := srv.ConfigureHTTP3(http3Addr, certFile, keyFile); err != nil {
		return fmt.Errorf("configure HTTP/3 server: %w", err)
	}

	// Listen for OS termination signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()

	// Both servers report their errors through this channel.
	type serverResult struct {
		name string
		err  error
	}

	serverErr := make(chan serverResult, 2)

	// Start the HTTP/1.1 server.
	go func() {
		serverErr <- serverResult{
			name: "HTTP/1.1",
			err:  srv.ListenAndServe(),
		}
	}()

	// Start the HTTP/3 server.
	go func() {
		serverErr <- serverResult{
			name: "HTTP/3",
			err:  srv.ListenAndServeHTTP3(),
		}
	}()

	log.Printf("Q-Cache HTTP/1.1 server listening on %s", http1Addr)
	log.Printf("Q-Cache HTTP/3 server listening on %s", http3Addr)

	select {

	case result := <-serverErr:

		// A server stopped unexpectedly.
		log.Printf("%s server stopped unexpectedly: %v", result.name, result.err)

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		shutdownErr := srv.Shutdown(shutdownCtx)
		cancel()

		// Wait for the other server to stop.
		other := <-serverErr

		if result.err == nil {
			result.err = errors.New("server stopped without an error")
		}

		resultErr := fmt.Errorf("%s server failed: %w", result.name, result.err)

		if other.err != nil && !errors.Is(other.err, http.ErrServerClosed) {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("%s server failed: %w", other.name, other.err),
			)
		}

		if shutdownErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("shutdown servers: %w", shutdownErr),
			)
		}

		return resultErr

	case <-ctx.Done():

		log.Println("Shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		shutdownErr := srv.Shutdown(shutdownCtx)
		cancel()

		// Wait for both servers to finish.
		var serveErr error

		for i := 0; i < 2; i++ {
			result := <-serverErr

			if result.err != nil &&
				!errors.Is(result.err, http.ErrServerClosed) {

				serveErr = errors.Join(
					serveErr,
					fmt.Errorf(
						"%s server stopped unexpectedly: %w",
						result.name,
						result.err,
					),
				)
			}
		}

		if err := errors.Join(shutdownErr, serveErr); err != nil {
			return fmt.Errorf("shutdown Q-Cache: %w", err)
		}

		log.Println("Q-Cache shut down successfully")

		return nil
	}
}
