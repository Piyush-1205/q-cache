package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Server manages both HTTP/1.1 and HTTP/3 servers.
type Server struct {
	httpServer      *http.Server
	http3Server     *http3.Server
	http3PacketConn net.PacketConn
}

// New creates the existing HTTP/1.1 server.
func New(addr string, handler http.Handler) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 20,
		},
	}
}

// ConfigureHTTP3 prepares the HTTP/3 server.
func (s *Server) ConfigureHTTP3(
	addr string,
	certFile string,
	keyFile string,
) error {
	if s.http3Server != nil {
		return errors.New("HTTP/3 server is already configured")
	}

	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load HTTP/3 TLS certificate: %w", err)
	}

	packetConn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("listen for HTTP/3 on %s: %w", addr, err)
	}

	tlsConfig := http3.ConfigureTLSConfig(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
	})

	s.http3Server = &http3.Server{
		Addr:           addr,
		Handler:        s.httpServer.Handler,
		TLSConfig:      tlsConfig,
		QUICConfig: &quic.Config{
			Allow0RTT: false,
		},
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	s.http3PacketConn = packetConn

	return nil
}

// ListenAndServe starts the HTTP/1.1 server.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// ListenAndServeHTTP3 starts the HTTP/3 server.
func (s *Server) ListenAndServeHTTP3() error {
	if s.http3Server == nil || s.http3PacketConn == nil {
		return errors.New("HTTP/3 server is not configured")
	}

	return s.http3Server.Serve(s.http3PacketConn)
}

// Shutdown gracefully stops both servers.
// Shutdown gracefully stops both servers.
func (s *Server) Shutdown(ctx context.Context) error {
	http1ErrCh := make(chan error, 1)

	// Start HTTP/1.1 graceful shutdown.
	go func() {
		http1ErrCh <- s.httpServer.Shutdown(ctx)
	}()

	var http3ErrCh chan error

	// Start HTTP/3 graceful shutdown.
	if s.http3Server != nil {
		http3ErrCh = make(chan error, 1)

		go func() {
			http3ErrCh <- s.http3Server.Shutdown(ctx)
		}()
	}

	// Wait for HTTP/1.1 shutdown.
	http1Err := <-http1ErrCh

	// If graceful shutdown failed or timed out,
	// force-close remaining HTTP/1.1 connections.
	if http1Err != nil {
		if err := s.httpServer.Close(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			http1Err = errors.Join(
				http1Err,
				fmt.Errorf("force close HTTP/1.1: %w", err),
			)
		}
	}

	var http3Err error

	// Wait for HTTP/3 shutdown.
	if http3ErrCh != nil {
		http3Err = <-http3ErrCh
	}

	// Close the UDP socket because we created and own it.
	var packetErr error

	if s.http3PacketConn != nil {
		if err := s.http3PacketConn.Close(); err != nil &&
			!errors.Is(err, net.ErrClosed) {
			packetErr = fmt.Errorf(
				"close HTTP/3 UDP socket: %w",
				err,
			)
		}
	}

	if http1Err != nil {
		http1Err = fmt.Errorf(
			"shutdown HTTP/1.1 server: %w",
			http1Err,
		)
	}

	if http3Err != nil {
		http3Err = fmt.Errorf(
			"shutdown HTTP/3 server: %w",
			http3Err,
		)
	}

	return errors.Join(http1Err, http3Err, packetErr)
}
