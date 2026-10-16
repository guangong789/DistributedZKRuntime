package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"fmt"
	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
	jobruntime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"os"
	"time"
)

// Process-local run identity; each Go calibration/final invocation also has its
// own existing invocation ID and fresh database. Never aggregate by JobID alone.
var submitBenchmarkRunID = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())

func identifySubmitBenchmarkSample(sample *submitBenchmarkSample, invocation, cohort string, iteration int) {
	sample.RunID, sample.Invocation = submitBenchmarkRunID, invocation
	sample.Cohort, sample.Iteration = cohort, iteration
}

// Stop returning alone does not join handlers. Only GracefulStop completion
// confirms that all handlers (including synchronous Prove) have returned.
// Both contexts share one overall Worker budget; the earlier graceful deadline
// reserves time to observe forced Stop, Serve and the original graceful waiter.
func stopSubmitBenchmarkWorker(gracefulCtx, cleanupCtx context.Context, gracefulStop, forceStop func(), serveDone <-chan struct{}) error {
	gracefulDone := make(chan struct{})
	go func() { defer close(gracefulDone); gracefulStop() }()
	graceful, serve, forced := (<-chan struct{})(gracefulDone), serveDone, (<-chan struct{})(nil)
	wait := func(ctx context.Context) {
		for graceful != nil || serve != nil || forced != nil {
			select {
			case <-graceful:
				graceful = nil
			case <-serve:
				serve = nil
			case <-forced:
				forced = nil
			case <-ctx.Done():
				return
			}
		}
	}
	// Include completions already observable at the deadline even if select
	// chose ctx.Done first. Nil also means the completion was observed above.
	completed := func(done <-chan struct{}) bool {
		if done == nil {
			return true
		}
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	wait(gracefulCtx)
	if completed(graceful) && completed(serve) {
		return nil
	}
	forcedDone := make(chan struct{})
	forced = forcedDone
	go func() { defer close(forcedDone); forceStop() }()
	wait(cleanupCtx)
	gracefulComplete, forcedComplete, serveExited := completed(graceful), completed(forced), completed(serve)
	handlers := "unknown"
	if gracefulComplete {
		handlers = "confirmed"
	}
	return fmt.Errorf("graceful shutdown deadline exceeded; forced_stop_attempted=true forced_stop_complete=%t serve_exited=%t graceful_stop_complete=%t handler_quiescence=%s",
		forcedComplete, serveExited, gracefulComplete, handlers)
}

// All configuration, events and trackers below are benchmark/test-only.
type submitBenchmarkConfig struct {
	workers, capacity int
	keyDir            string
	track             bool
}

type capacityProofCounts struct{ started, completed, active, peak, afterCancel atomic.Int64 }
type capacityProofSnapshot struct {
	Started     int64 `json:"started"`
	Completed   int64 `json:"completed"`
	Active      int64 `json:"active"`
	Peak        int64 `json:"peak"`
	AfterCancel int64 `json:"completed_after_context_cancel"`
}

func (c *capacityProofCounts) begin() {
	c.started.Add(1)
	active := c.active.Add(1)
	for peak := c.peak.Load(); active > peak; peak = c.peak.Load() {
		if c.peak.CompareAndSwap(peak, active) {
			break
		}
	}
}
func (c *capacityProofCounts) end(cancelled bool) {
	if cancelled {
		c.afterCancel.Add(1)
	}
	c.completed.Add(1)
	c.active.Add(-1)
}
func (c *capacityProofCounts) snapshot() capacityProofSnapshot {
	return capacityProofSnapshot{c.started.Load(), c.completed.Load(), c.active.Load(), c.peak.Load(), c.afterCancel.Load()}
}
func (c *capacityProofCounts) reset() {
	c.started.Store(0)
	c.completed.Store(0)
	c.active.Store(0)
	c.peak.Store(0)
	c.afterCancel.Store(0)
}

type capacityEvent struct {
	Kind      string `json:"kind"`
	JobID     int64  `json:"job_id"`
	AttemptID int64  `json:"attempt_id"`
	Code      string `json:"rpc_code,omitempty"`
	StartNS   int64  `json:"start_ns_from_fixture,omitempty"`
	EndNS     int64  `json:"end_ns_from_fixture,omitempty"`
	Cancelled bool   `json:"context_cancelled_at_prove_return,omitempty"`
	Proved    bool   `json:"prove_returned_without_error,omitempty"`
}
type submitBenchmarkWorker struct {
	prover      *zk.PreimageProver
	witnesses   jobruntime.WitnessStore
	id          string
	server      *grpc.Server
	done        chan struct{}
	registry    *prom.Registry
	limiter     *jobruntime.ProvingLimiter
	calls, busy atomic.Int32
	proofs      capacityProofCounts
	events      chan capacityEvent
	dropped     atomic.Int64
}

func (w *submitBenchmarkWorker) record(e capacityEvent) {
	select {
	case w.events <- e:
	default:
		w.dropped.Add(1)
	}
}
func (w *submitBenchmarkWorker) drainEvents() []capacityEvent {
	var events []capacityEvent
	for {
		select {
		case e := <-w.events:
			events = append(events, e)
		default:
			return events
		}
	}
}

type capacityTrackedProver struct {
	ctx              context.Context
	delegate         jobruntime.PreimageProver
	worker           *submitBenchmarkWorker
	all              *capacityProofCounts
	origin           time.Time
	jobID, attemptID int64
}

func (p *capacityTrackedProver) Prove(secret int64, digest fr.Element) ([]byte, error) {
	p.worker.proofs.begin()
	p.all.begin()
	start := time.Since(p.origin).Nanoseconds()
	proof, err := p.delegate.Prove(secret, digest)
	end := time.Since(p.origin).Nanoseconds()
	cancelled := p.ctx.Err() != nil
	p.all.end(cancelled)
	p.worker.proofs.end(cancelled)
	p.worker.record(capacityEvent{Kind: "prove", JobID: p.jobID, AttemptID: p.attemptID, StartNS: start, EndNS: end, Cancelled: cancelled, Proved: err == nil})
	return proof, err
}

func loadCapacityCrypto(t testing.TB, dir string) (integrationZKFixture, map[string]string) {
	t.Helper()
	hashes := make(map[string]string)
	for _, name := range []string{"proving.key", "verifying.key"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read benchmark %s: %v", name, err)
		}
		digest := sha256.Sum256(data)
		hashes[name] = hex.EncodeToString(digest[:])
	}
	prover, err := zk.LoadPreimageProver(filepath.Join(dir, "proving.key"))
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := zk.LoadPreimageVerifier(filepath.Join(dir, "verifying.key"))
	if err != nil {
		t.Fatal(err)
	}
	ccs, err := frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, &zk.PreimageCircuit{})
	if err != nil || ccs.GetNbConstraints() != 331 {
		t.Fatal("benchmark requires the fixed 331-constraint MiMC circuit")
	}
	digest := zk.ComputePreimageDigest(42)
	proof, err := prover.Prove(42, digest)
	if err != nil {
		t.Fatal("benchmark key validation Prove failed")
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatal("benchmark proving/verifying keys do not match")
	}
	witnesses := jobruntime.NewMemoryWitnessStore()
	witnesses.Put("secret-001", 42)
	return integrationZKFixture{prover: prover, verifier: verifier, witnesses: witnesses, payload: preimagePublicPayload(digest)}, hashes
}

type capacityStopTarget struct {
	id              string
	graceful, force func()
	serve           <-chan struct{}
}
type capacityWorkerCleanup struct {
	WorkerID string `json:"worker_id"`
	Joined   bool   `json:"clean_shutdown"`
	Error    string `json:"error,omitempty"`
}
type capacityCleanup struct {
	CoordinatorOK bool                    `json:"coordinator_shutdown_and_sqlite_close_ok"`
	Workers       []capacityWorkerCleanup `json:"workers"`
	ConnectionsOK bool                    `json:"connections_closed"`
}

func stopSubmitBenchmarkWorkers(gracefulCtx, cleanupCtx context.Context, targets []capacityStopTarget) []capacityWorkerCleanup {
	results := make(chan capacityWorkerCleanup, len(targets))
	for _, target := range targets {
		go func() {
			err := stopSubmitBenchmarkWorker(gracefulCtx, cleanupCtx, target.graceful, target.force, target.serve)
			result := capacityWorkerCleanup{WorkerID: target.id, Joined: err == nil}
			if err != nil {
				result.Error = err.Error()
			}
			results <- result
		}()
	}
	observed := make(map[string]capacityWorkerCleanup)
collect:
	for len(observed) < len(targets) {
		select {
		case result := <-results:
			observed[result.WorkerID] = result
		case <-cleanupCtx.Done():
			for {
				select {
				case result := <-results:
					observed[result.WorkerID] = result
				default:
					break collect
				}
			}
		}
	}
	ordered := make([]capacityWorkerCleanup, 0, len(targets))
	for _, target := range targets {
		result, ok := observed[target.id]
		if !ok {
			result = capacityWorkerCleanup{WorkerID: target.id, Error: "shared cleanup deadline; Stop, Serve and handler completion unobserved/unknown"}
		}
		ordered = append(ordered, result)
	}
	return ordered
}
func (f *submitBenchmarkFixture) finish() error {
	f.finishOnce.Do(func() {
		f.cancel()
		f.cleanup.CoordinatorOK = true
		if f.process != nil {
			if err := f.process.shutdown(); err != nil {
				f.cleanup.CoordinatorOK = false
				f.finishErr = errors.Join(f.finishErr, fmt.Errorf("Coordinator shutdown/SQLite closure: %w", err))
			}
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), submitBenchCleanup)
		defer cancel()
		gracefulCtx, cancelGraceful := context.WithTimeout(cleanupCtx, submitBenchCleanup/2)
		defer cancelGraceful()
		var targets []capacityStopTarget
		for _, w := range f.workers {
			if w.server != nil {
				targets = append(targets, capacityStopTarget{w.id, w.server.GracefulStop, w.server.Stop, w.done})
			}
		}
		f.cleanup.Workers = stopSubmitBenchmarkWorkers(gracefulCtx, cleanupCtx, targets)
		for _, result := range f.cleanup.Workers {
			if !result.Joined {
				f.finishErr = errors.Join(f.finishErr, fmt.Errorf("Worker %s cleanup: %s", result.WorkerID, result.Error))
			}
		}
		f.cleanup.ConnectionsOK = true
		for _, conn := range f.connections {
			if err := conn.Close(); err != nil {
				f.cleanup.ConnectionsOK = false
				f.finishErr = errors.Join(f.finishErr, fmt.Errorf("close benchmark connection: %w", err))
			}
		}
	})
	return f.finishErr
}
