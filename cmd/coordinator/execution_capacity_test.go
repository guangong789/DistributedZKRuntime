package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"encoding/base64"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	jobruntime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

// This barrier is used only by the correctness gate, never by the benchmark's
// proving path. The inner tracker measures real call lifetimes after release.
type capacityGateProver struct {
	delegate jobruntime.PreimageProver
	ready    chan<- struct{}
	release  <-chan struct{}
	ctx      context.Context
}

func (p capacityGateProver) Prove(secret int64, digest fr.Element) ([]byte, error) {
	select {
	case p.ready <- struct{}{}:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
	select {
	case <-p.release:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
	return p.delegate.Prove(secret, digest)
}
func newCapacityKeyDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	if err := zk.GeneratePreimageSetup(filepath.Join(dir, "proving.key"), filepath.Join(dir, "verifying.key")); err != nil {
		t.Fatal(err)
	}
	return dir
}
func TestCapacitySharedRealProver(t *testing.T) {
	crypto := newIntegrationZKFixture(t)
	limiter, err := jobruntime.NewProvingLimiter(2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ready, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	w := &submitBenchmarkWorker{events: make(chan capacityEvent, 4)}
	var all capacityProofCounts
	results := make(chan jobruntime.Result, 2)
	origin := time.Now()
	for id := int64(1); id <= 2; id++ {
		go func() {
			real := &capacityTrackedProver{ctx: ctx, delegate: crypto.prover, worker: w, all: &all, origin: origin, jobID: id, attemptID: id}
			task := jobruntime.ZKPreimageTask{Payload: crypto.payload, WitnessStore: crypto.witnesses, Limiter: limiter,
				Prover: capacityGateProver{delegate: real, ready: ready, release: release, ctx: ctx}}
			results <- jobruntime.ExecuteJob(ctx, jobruntime.Job{ID: int(id), Status: jobruntime.JobRunning, Task: task})
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("two real tasks did not acquire capacity=2")
		}
	}
	unblock()
	seen := make(map[int]bool)
	for range 2 {
		select {
		case result := <-results:
			if result.Err != nil || result.Status != jobruntime.JobSucceeded || seen[result.JobID] {
				t.Fatalf("shared Prove result: id=%d status=%v error=%v", result.JobID, result.Status, result.Err)
			}
			seen[result.JobID] = true
			proof, err := base64.StdEncoding.DecodeString(result.Output)
			if err != nil {
				t.Fatal(err)
			}
			if err := crypto.verifier.Verify(proof, zk.ComputePreimageDigest(42)); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("shared real Prove did not finish")
		}
	}
	snapshot := all.snapshot()
	if snapshot.Started != 2 || snapshot.Completed != 2 || snapshot.Active != 0 || snapshot.Peak != 2 {
		t.Fatalf("real call overlap not established: %+v", snapshot)
	}
	events := w.drainEvents()
	if len(events) != 2 || !(events[0].StartNS < events[1].EndNS && events[1].StartNS < events[0].EndNS) {
		t.Fatalf("real Prove call intervals did not overlap: %+v", events)
	}
	if !limiter.TryAcquire() {
		t.Fatal("first proving permit leaked")
	}
	if !limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("second proving permit leaked")
	}
	if limiter.TryAcquire() {
		t.Fatal("limiter capacity increased")
	}
	limiter.Release()
	limiter.Release()
	t.Logf("two proofs independently verified; real wrapper call intervals overlap; peak=%d", snapshot.Peak)
}
func TestCapacityTwoWorkerFixture(t *testing.T) {
	f := newConfiguredSubmitBenchmarkFixture(t, submitBenchmarkConfig{workers: 2, capacity: 1, keyDir: newCapacityKeyDir(t), track: true})
	if len(f.workers) != 2 || f.workers[0].limiter == f.workers[1].limiter || f.workers[0].registry == f.workers[1].registry || f.workers[0].prover == f.workers[1].prover || f.workers[0].witnesses == f.workers[1].witnesses {
		t.Fatal("Worker resources are not independent")
	}
	for id := int64(1); id <= 2; id++ {
		sample := runCapacityRequest(f, "test", "D", "warmup", 0, int(id), id, time.Now())
		if sample.RPCCode != "OK" || sample.ResponseState != "Succeeded" {
			t.Fatalf("real mTLS dispatch failed: %+v", sample)
		}
	}
	if f.workers[0].calls.Load() != 1 || f.workers[1].calls.Load() != 1 {
		t.Fatal("both registered workers were not selected")
	}
	if err := f.finish(); err != nil {
		t.Fatal(err)
	}
	for _, w := range f.workers {
		if w.proofs.snapshot().Active != 0 {
			t.Fatal("residual Prove after joined cleanup")
		}
	}
}

// Accounting must accept a durably Failed busy-exhausted job without claiming
// that either published attempt entered Prove. Use the real metrics backend.
func TestCapacityBusyAccounting(t *testing.T) {
	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	before := gatherIntegrationMetrics(t, registry)
	observer.JobSubmitted(metrics.SourceSubmit)
	for range 2 {
		observer.AttemptStarted(metrics.SourceSubmit)
		observer.AttemptFinished(metrics.SourceSubmit, metrics.AttemptRPCError, time.Millisecond)
	}
	observer.JobTerminal(metrics.SourceSubmit, metrics.TerminalFailed)
	after := gatherIntegrationMetrics(t, registry)
	for _, tc := range []struct {
		name                 string
		change               string
		wantOK, wantExpected bool
	}{
		{"busy_exhaustion", "", true, true}, {"transport_error_is_distinct", "transport", true, false},
		{"missing_durable_record", "missing", false, true}, {"dropped_event", "dropped", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := capacityReport{Iterations: 1, Issued: 1, Attempts: 2, Calls: 2, Busy: 2, RPCStatus: make(map[string]int), Durable: make(map[string]int),
				Samples: []capacitySample{{submitBenchmarkSample: submitBenchmarkSample{JobID: 7, RPCCode: "Unavailable", DurableState: "Failed", DurableAttempt: 2}, Issued: true, SpecMatches: true}},
				Workers: []capacityWorkerReport{{WorkerID: "worker-a", Capacity: 1, Calls: 2, Busy: 2, Events: []capacityEvent{{Kind: "rpc", JobID: 7, AttemptID: 1, Code: "ResourceExhausted"}, {Kind: "rpc", JobID: 7, AttemptID: 2, Code: "ResourceExhausted"}}}}}
			switch tc.change {
			case "transport":
				r.Workers[0].Events[0].Code = "Unavailable"
				r.Workers[0].Busy = 1
				r.Busy = 1
			case "missing":
				r.Samples[0].DurableState = "Missing"
			case "dropped":
				r.Workers[0].Dropped = 1
			}
			finalizeCapacityAccounting(&r, before, after)
			if r.AccountingOK != tc.wantOK || r.ExpectedOutcomes != tc.wantExpected {
				t.Fatalf("accounting=%t expected-capacity-outcome=%t", r.AccountingOK, r.ExpectedOutcomes)
			}
			if r.Proofs.Started != 0 || r.Attempts != 2 {
				t.Fatal("busy attempts were counted as proving")
			}
		})
	}
}

type capacityProveFunc func(int64, fr.Element) ([]byte, error)

func (fn capacityProveFunc) Prove(secret int64, digest fr.Element) ([]byte, error) {
	return fn(secret, digest)
}
func TestCapacityTrackerForwardsResultAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelled {
				cancel()
			}
			sentinel := errors.New("test prover failure")
			var wantErr error
			if !cancelled {
				wantErr = sentinel
			}
			output := []byte("opaque-test-only-proof")
			digest := zk.ComputePreimageDigest(42)
			w := &submitBenchmarkWorker{events: make(chan capacityEvent, 1)}
			var all capacityProofCounts
			tracker := capacityTrackedProver{ctx: ctx, worker: w, all: &all, origin: time.Now(), jobID: 7, attemptID: 2,
				delegate: capacityProveFunc(func(secret int64, got fr.Element) ([]byte, error) {
					if secret != 42 || !got.Equal(&digest) {
						t.Fatal("tracker changed prover inputs")
					}
					return output, wantErr
				})}
			got, err := tracker.Prove(42, digest)
			if !bytes.Equal(got, output) || err != wantErr {
				t.Fatal("tracker changed prover result/error")
			}
			snap := all.snapshot()
			wantCancelled := int64(0)
			if cancelled {
				wantCancelled = 1
			}
			if snap.Started != 1 || snap.Completed != 1 || snap.Active != 0 || snap.AfterCancel != wantCancelled {
				t.Fatalf("tracker counters=%+v", snap)
			}
		})
	}
}

func TestCapacityGlobalPeakAcrossWorkers(t *testing.T) {
	var first, second, global capacityProofCounts
	first.begin()
	global.begin()
	second.begin()
	global.begin()
	first.end(false)
	global.end(false)
	second.end(false)
	global.end(false)
	if first.snapshot().Peak != 1 || second.snapshot().Peak != 1 || global.snapshot().Peak != 2 || global.snapshot().Active != 0 {
		t.Fatal("shared tracker did not count both Workers")
	}
	for _, tc := range []struct {
		name       string
		secondFrom int64
		wantPeak   int
	}{{"overlap", 20, 2}, {"touching", 30, 1}, {"disjoint", 40, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			// Synthetic call intervals test aggregation only, not a full RPC cohort.
			r := capacityReport{Workers: []capacityWorkerReport{
				{WorkerID: "worker-a", Events: []capacityEvent{{Kind: "prove", StartNS: 10, EndNS: 30}}},
				{WorkerID: "worker-b", Events: []capacityEvent{{Kind: "prove", StartNS: tc.secondFrom, EndNS: 50}}},
			}}
			finalizeCapacityAccounting(&r, nil, nil)
			if r.Workers[0].IntervalPeak != 1 || r.Workers[1].IntervalPeak != 1 || r.IntervalPeak != tc.wantPeak {
				t.Fatalf("per-Worker peaks=%d,%d global=%d; want 1,1,%d", r.Workers[0].IntervalPeak, r.Workers[1].IntervalPeak, r.IntervalPeak, tc.wantPeak)
			}
		})
	}
}
