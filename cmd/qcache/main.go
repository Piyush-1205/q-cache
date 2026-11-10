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
	cache := storage.New()
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

	// Keep the development server local-only for now.
	const listenAddr = "127.0.0.1:8080"

	// Create the HTTP server.
	srv := server.New(listenAddr, handler)

	// Listen for OS termination signals.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErr := make(chan error, 1)

	// Start serving requests.
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	log.Printf("Q-Cache HTTP server listening on %s", listenAddr)

	select {

	case err := <-serverErr:

		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("HTTP server failed: %w", err)

	case <-ctx.Done():

		log.Println("Shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}

		if err := <-serverErr; err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server stopped unexpectedly: %w", err)
		}

		log.Println("Q-Cache shut down successfully")

		return nil
	}
}
