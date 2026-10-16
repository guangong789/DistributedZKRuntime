package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitBenchmarkWorkerCleanupNormal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	serveDone := make(chan struct{})
	var forced atomic.Bool
	err := stopSubmitBenchmarkWorker(ctx, ctx, func() { close(serveDone) }, func() { forced.Store(true) }, serveDone)
	if err != nil || forced.Load() {
		t.Fatalf("normal cleanup: err=%v forced=%t", err, forced.Load())
	}
	select {
	case <-serveDone:
	default:
		t.Fatal("Serve completion not observed")
	}
}

func TestSubmitBenchmarkWorkerCleanupForced(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	gracefulCtx, cancelGraceful := context.WithCancel(ctx)
	defer cancelGraceful()
	started, release, serveDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- stopSubmitBenchmarkWorker(gracefulCtx, ctx, func() { close(started); <-release }, func() {
			close(serveDone)
			close(release)
		}, serveDone)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("GracefulStop did not start")
	}
	cancelGraceful() // Deterministically expire grace only after it has started.
	select {
	case err := <-result:
		assertSubmitBenchmarkCleanupError(t, err, "forced_stop_complete=true", "serve_exited=true", "graceful_stop_complete=true", "handler_quiescence=confirmed")
	case <-ctx.Done():
		t.Fatal("forced cleanup failed to join completed components")
	}
}

func TestSubmitBenchmarkWorkerCleanupIncomplete(t *testing.T) {
	for _, blockForced := range []bool{false, true} {
		t.Run(map[bool]string{false: "handler_still_running", true: "forced_stop_and_serve_blocked"}[blockForced], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			gracefulCtx, cancelGraceful := context.WithCancel(ctx)
			defer cancelGraceful()
			// A real deadline bounds observation of a deliberately non-cooperative
			// handler; channels, rather than sleeps, establish the fault ordering.
			cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancelCleanup()
			started, forcedStarted := make(chan struct{}), make(chan struct{})
			release, serveDone := make(chan struct{}), make(chan struct{})
			gracefulReturned, forcedReturned := make(chan struct{}), make(chan struct{})
			defer func() {
				close(release)
				for _, done := range []<-chan struct{}{gracefulReturned, forcedReturned} {
					select {
					case <-done:
					case <-ctx.Done():
						t.Error("test callback did not exit after release")
					}
				}
			}()
			result := make(chan error, 1)
			go func() {
				result <- stopSubmitBenchmarkWorker(gracefulCtx, cleanupCtx, func() {
					defer close(gracefulReturned)
					close(started)
					<-release
				}, func() {
					defer close(forcedReturned)
					close(forcedStarted)
					if blockForced {
						<-release
					} else {
						close(serveDone)
					}
				}, serveDone)
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("GracefulStop did not start")
			}
			cancelGraceful()
			select {
			case <-forcedStarted:
			case <-ctx.Done():
				t.Fatal("forced Stop not attempted")
			}
			if blockForced {
				cancelCleanup() // Stop and Serve provably cannot complete yet.
			}
			select {
			case err := <-result:
				assertSubmitBenchmarkCleanupError(t, err, "graceful_stop_complete=false", "handler_quiescence=unknown")
				if blockForced {
					assertSubmitBenchmarkCleanupError(t, err, "forced_stop_complete=false", "serve_exited=false")
				}
			case <-ctx.Done():
				t.Fatal("incomplete cleanup exceeded overall observation bound")
			}
		})
	}
}

func assertSubmitBenchmarkCleanupError(t *testing.T, err error, fields ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("graceful deadline exceeded but cleanup reported success")
	}
	for _, field := range fields {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("cleanup error %q missing %q", err, field)
		}
	}
}

func TestSubmitBenchmarkSampleIdentity(t *testing.T) {
	// Reused numeric IDs remain distinct both across invocations and cohorts.
	samples := []submitBenchmarkSample{{JobID: 4}, {JobID: 4}, {JobID: 4}}
	identifySubmitBenchmarkSample(&samples[0], "calibration", "measured", 1)
	identifySubmitBenchmarkSample(&samples[1], "final", "measured", 1)
	identifySubmitBenchmarkSample(&samples[2], "final", "warmup", 1)
	encoded, err := json.Marshal(samples)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(encoded, &rows); err != nil {
		t.Fatal(err)
	}
	seen := make(map[[4]any]bool)
	for _, row := range rows {
		key := [4]any{row["run_id"], row["invocation"], row["cohort"], row["job_id"]}
		if row["run_id"] != submitBenchmarkRunID || row["iteration"] != float64(1) || seen[key] {
			t.Fatalf("ambiguous exported sample identity: %s", encoded)
		}
		seen[key] = true
	}
}

func TestCapacityCleanupServeStalls(t *testing.T) {
	for _, graceReturns := range []bool{true, false} {
		t.Run(map[bool]string{true: "graceful_returns_serve_blocks", false: "forced_returns_serve_blocks"}[graceReturns], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancelCleanup()
			graceCtx, cancelGrace := context.WithCancel(cleanupCtx)
			defer cancelGrace()
			started, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				close(release)
				select {
				case <-returned:
				case <-ctx.Done():
					t.Error("fake graceful operation leaked")
				}
			}()
			result := make(chan error, 1)
			go func() {
				result <- stopSubmitBenchmarkWorker(graceCtx, cleanupCtx, func() {
					defer close(returned)
					close(started)
					if !graceReturns {
						<-release
					}
				}, func() {}, make(chan struct{}))
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("graceful operation not started")
			}
			cancelGrace()
			select {
			case err := <-result:
				assertSubmitBenchmarkCleanupError(t, err, "forced_stop_complete=true", "serve_exited=false")
				if graceReturns {
					assertSubmitBenchmarkCleanupError(t, err, "handler_quiescence=confirmed")
				} else {
					assertSubmitBenchmarkCleanupError(t, err, "handler_quiescence=unknown")
				}
			case <-ctx.Done():
				t.Fatal("Serve stall exceeded cleanup bound")
			}
		})
	}
}

func TestCapacityCleanupSharedDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cleanupCtx, cancelCleanup := context.WithCancel(ctx)
	defer cancelCleanup()
	graceCtx, cancelGrace := context.WithCancel(cleanupCtx)
	defer cancelGrace()
	ready, forced, returned := make(chan struct{}, 2), make(chan struct{}, 2), make(chan struct{}, 4)
	release := make(chan struct{})
	defer func() {
		close(release)
		for range 4 {
			select {
			case <-returned:
			case <-ctx.Done():
				t.Error("fake shutdown callback leaked")
			}
		}
	}()
	var targets []capacityStopTarget
	for _, id := range []string{"worker-a", "worker-b"} {
		targets = append(targets, capacityStopTarget{id: id,
			graceful: func() { defer func() { returned <- struct{}{} }(); ready <- struct{}{}; <-release },
			force:    func() { defer func() { returned <- struct{}{} }(); forced <- struct{}{}; <-release }, serve: make(chan struct{})})
	}
	result := make(chan []capacityWorkerCleanup, 1)
	go func() { result <- stopSubmitBenchmarkWorkers(graceCtx, cleanupCtx, targets) }()
	// Both operations must start before either deadline is released; a serial
	// implementation cannot satisfy these channel handshakes.
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("Worker shutdown was serialized")
		}
	}
	cancelGrace()
	for range 2 {
		select {
		case <-forced:
		case <-ctx.Done():
			t.Fatal("both forced Stops were not started")
		}
	}
	cancelCleanup()
	select {
	case reports := <-result:
		if len(reports) != 2 {
			t.Fatal("missing per-Worker shutdown reports")
		}
		for i, report := range reports {
			if report.WorkerID != targets[i].id || report.Joined || report.Error == "" {
				t.Fatalf("incorrect Worker cleanup report: %+v", report)
			}
		}
	case <-ctx.Done():
		t.Fatal("shared deadline did not bound shutdown")
	}
}
