package prometheus

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
)

func TestHTTPHandlerUsesSuppliedRegistry(t *testing.T) {
	backend, registry := newTestBackend(t)
	backend.JobSubmitted(metrics.SourceSubmit)
	server := httptest.NewServer(Handler(registry))
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK ||
		!strings.Contains(string(body), `dzkr_jobs_submitted_total{source="submit"} 1`) ||
		!strings.Contains(string(body), "dzkr_zk_verification_duration_seconds") {
		t.Fatalf("metrics response: status=%d body=%s", resp.StatusCode, body)
	}
	respOther, err := client.Get(server.URL + "/other")
	if err != nil {
		t.Fatal(err)
	}
	defer respOther.Body.Close()
	if respOther.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected route status=%d", respOther.StatusCode)
	}
}

func TestHTTPListenerStartupAndShutdown(t *testing.T) {
	backend, registry := newTestBackend(t)
	server, err := StartHTTP("127.0.0.1:0", registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	boundAddress := server.Addr()
	host, _, err := net.SplitHostPort(boundAddress)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		t.Fatalf("test listener is not loopback: %s error=%v", server.Addr(), err)
	}
	backend.JobSubmitted(metrics.SourceRecovery)
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK ||
		!strings.Contains(string(body), `dzkr_jobs_submitted_total{source="recovery"} 1`) {
		t.Fatalf("HTTP metrics status=%d error=%v body=%s", resp.StatusCode, err, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown metrics server: %v", err)
	}
	if server.Addr() != boundAddress {
		t.Fatal("listener address changed after shutdown")
	}
	select {
	case <-server.done:
	default:
		t.Fatal("metrics Serve remained running after shutdown")
	}
	if _, err := server.listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("original metrics listener did not close: %v", err)
	}
}

func TestHTTPStartupErrors(t *testing.T) {
	registry := prom.NewRegistry()
	for _, tc := range []struct {
		address string
		reg     prom.Gatherer
	}{
		{"", registry},
		{"127.0.0.1:0", nil},
		{"invalid-address", registry},
	} {
		if server, err := StartHTTP(tc.address, tc.reg); err == nil || server != nil {
			t.Fatalf("invalid configuration started server: address=%q error=%v", tc.address, err)
		}
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	if server, err := StartHTTP(occupied.Addr().String(), registry); err == nil || server != nil {
		t.Fatal("occupied metrics listener did not fail startup")
	}
}

func TestHTTPShutdownForceClosesOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})}
	done := make(chan struct{})
	server := &HTTPServer{server: httpServer, listener: listener, done: done}
	go func() {
		defer close(done)
		_ = httpServer.Serve(listener)
	}()
	t.Cleanup(func() { _ = httpServer.Close() })
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://" + server.Addr() + "/metrics")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	select {
	case <-entered:
	case <-waitCtx.Done():
		t.Fatal("test request did not enter handler")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error=%v, want context.Canceled", err)
	}
	select {
	case <-requestDone:
	case <-waitCtx.Done():
		t.Fatal("forced shutdown left the client request blocked")
	}
	once.Do(func() { close(release) })
}

func TestHTTPImmediateShutdownWaitsForServe(t *testing.T) {
	for i := 0; i < 10; i++ {
		server, err := StartHTTP("127.0.0.1:0", prom.NewRegistry())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = server.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatalf("immediate shutdown: %v", err)
		}
		select {
		case <-server.done:
		default:
			t.Fatal("Shutdown returned before Serve exited")
		}
		if _, err := server.listener.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("immediate shutdown left listener open: %v", err)
		}
	}
}
