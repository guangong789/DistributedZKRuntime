package prometheus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServer struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
}

// Handler exposes only /metrics using the supplied gatherer, never a global
// registry. Counter/vector series appear after their first semantic event.
func Handler(gatherer prom.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return mux
}

// StartHTTP binds synchronously so invalid or occupied addresses fail startup.
// Callers choose the address explicitly; application defaults are loopback only.
func StartHTTP(address string, gatherer prom.Gatherer) (*HTTPServer, error) {
	if address == "" || gatherer == nil {
		return nil, fmt.Errorf("metrics listen address and gatherer are required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics on %s: %w", address, err)
	}
	server := &http.Server{
		Handler:           Handler(gatherer),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics HTTP server: %v", err)
		}
	}()
	return &HTTPServer{server: server, listener: listener, done: done}, nil
}

func (s *HTTPServer) Addr() string { return s.listener.Addr().String() }

// Shutdown drains requests until ctx expires, then force-closes connections.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	if err != nil {
		_ = s.server.Close()
	}
	// Shutdown can race with the Serve goroutine registering its listener.
	// Close the bound listener explicitly, then wait for Serve within the limit.
	_ = s.listener.Close()
	if err != nil {
		return err
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		_ = s.server.Close()
		return ctx.Err()
	}
}
