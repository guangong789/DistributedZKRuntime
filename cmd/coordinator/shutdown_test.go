package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Records real SQLite access and Close. Hooks block only the selected Save;
// instrumentation uses a separate lock from Coordinator.mu.
type shutdownStore struct {
	JobStore
	mu           sync.Mutex
	closed       bool
	afterClose   int
	events       []string
	beforeSave   func(JobRecord)
	closeBacking func() error
}

func (s *shutdownStore) access(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.afterClose++
	}
	s.events = append(s.events, name)
}
func (s *shutdownStore) Load(id int64) (JobRecord, bool, error) {
	s.access("load")
	return s.JobStore.Load(id)
}
func (s *shutdownStore) ListByState(state JobState) ([]JobRecord, error) {
	s.access("list")
	return s.JobStore.ListByState(state)
}
func (s *shutdownStore) Save(record JobRecord) error {
	s.access("save")
	if s.beforeSave != nil {
		s.beforeSave(record)
	}
	return s.JobStore.Save(record)
}
func (s *shutdownStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.events = append(s.events, "close")
	s.mu.Unlock()
	return s.closeBacking()
}
func (s *shutdownStore) snapshot() (bool, int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed, s.afterClose, append([]string(nil), s.events...)
}

func shutdownFixture(t *testing.T, timeout time.Duration, beforeSave func(JobRecord), configure ...func(*mtlsCoordinator, *coordinatorProcess)) (*mtlsCoordinator, *coordinatorProcess, *shutdownStore, string) {
	t.Helper()
	f := newMTLSCoordinator(t)
	db := f.store.JobStore.(*SQLiteJobStore)
	var seq int
	var name, path string
	if err := db.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	store := &shutdownStore{JobStore: f.store, beforeSave: beforeSave, closeBacking: db.Close}
	f.s.jobStore = store // Configure before starting any requests or recovery.
	p := newCoordinatorProcess(f.s, timeout)
	p.server, p.serveDone = f.server, f.serveDone
	p.closeStore = store.Close
	p.shutdownMetrics = func(ctx context.Context) error {
		if ctx.Err() != nil || f.s.lifecycle.ctx.Err() == nil {
			t.Error("cleanup context inherited runtime cancellation")
		}
		store.access("metrics")
		return nil
	}
	for _, setup := range configure {
		setup(f, p)
	}
	p.startBackground()
	t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
	return f, p, store, path
}

func awaitShutdownSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown barrier did not complete")
	}
}
func assertShutdownStoreClosed(t *testing.T, store *shutdownStore) {
	t.Helper()
	closed, after, events := store.snapshot()
	if !closed || after != 0 || len(events) < 2 || events[len(events)-2] != "metrics" || events[len(events)-1] != "close" {
		t.Fatalf("unsafe cleanup: closed=%t afterClose=%d events=%v", closed, after, events)
	}
}
func loadShutdownRecord(t *testing.T, path string, want JobRecord) {
	t.Helper()
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertRecoveryRecord(t, store, want)
}

func TestCoordinatorShutdownTimeoutConfiguration(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 30 * time.Second}, {[]string{"-shutdown-timeout", "75ms"}, 75 * time.Millisecond},
	} {
		fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
		cfg := coordinatorExecutionFlags(fs)
		if err := fs.Parse(tc.args); err != nil {
			t.Fatal(err)
		}
		if err := cfg.validate(); err != nil || cfg.ShutdownTimeout != tc.want {
			t.Fatalf("config=%+v error=%v", cfg, err)
		}
	}
}

func TestCoordinatorShutdownIdle(t *testing.T) {
	_, p, store, _ := shutdownFixture(t, time.Second, nil)
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	awaitShutdownSignal(t, p.cleanupDone)
	assertShutdownStoreClosed(t, store)
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	_, _, events := store.snapshot()
	closes := 0
	for _, event := range events {
		if event == "close" {
			closes++
		}
	}
	if closes != 1 {
		t.Fatal("shutdown repeated resource cleanup")
	}
}

func TestCoordinatorShutdownAdmissionAndContexts(t *testing.T) {
	l := newCoordinatorLifecycle()
	type key struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "value"), time.Minute)
	defer cancel()
	identity := &peer.Peer{}
	parent = peer.NewContext(metadata.NewIncomingContext(parent, metadata.Pairs("example", "kept")), identity)
	first, leaveFirst, ok := l.tryEnter(parent)
	if !ok {
		t.Fatal("initial admission denied")
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, leave, ok := l.tryEnter(parent)
			if ok {
				defer leave()
				<-l.ctx.Done()
				<-ctx.Done()
			}
		}()
	}
	close(start)
	l.beginShutdown()
	l.cancelWork()
	wg.Wait()
	if _, _, ok := l.tryEnter(parent); ok {
		t.Fatal("admission reopened")
	}
	if first.Value(key{}) != "value" {
		t.Fatal("RPC value lost")
	}
	md, _ := metadata.FromIncomingContext(first)
	gotPeer, _ := peer.FromContext(first)
	d1, _ := first.Deadline()
	d2, _ := parent.Deadline()
	if md.Get("example")[0] != "kept" || gotPeer != identity || d1 != d2 || !errors.Is(first.Err(), context.Canceled) {
		t.Fatal("RPC context replaced rather than derived")
	}
	select {
	case <-l.done:
		t.Fatal("admitted handler not tracked through cleanup")
	default:
	}
	leaveFirst()
	leaveFirst()
	awaitShutdownSignal(t, l.done)
	l.beginShutdown()
	// The original RPC parent must also cancel an admitted request independently.
	other := newCoordinatorLifecycle()
	rpc, cancelRPC := context.WithCancel(context.Background())
	child, leave, _ := other.tryEnter(rpc)
	cancelRPC()
	if !errors.Is(child.Err(), context.Canceled) {
		t.Fatal("parent cancellation lost")
	}
	leave()
	other.beginShutdown()
	other.cancelWork()
	awaitShutdownSignal(t, other.done)
}

func TestCoordinatorShutdownRejectsBusinessAfterAuthorization(t *testing.T) {
	f := newMTLSCoordinator(t)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	worker := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	f.s.lifecycle.beginShutdown() // Transport stays open to exercise the gate.
	before := f.snapshot()
	ctx := mtlsContext(t)
	_, submitErr := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9701, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	_, registerErr := worker.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "127.0.0.1:50051"})
	_, heartbeatErr := worker.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"})
	for _, err := range []error{submitErr, registerErr, heartbeatErr} {
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("drain error=%v", err)
		}
	}
	_, err := worker.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9701})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("draining bypassed authorization: %v", err)
	}
	f.assertUnchanged(t, before, 9701)
	f.s.lifecycle.cancelWork()
	awaitShutdownSignal(t, f.s.lifecycle.done)
}

func TestCoordinatorShutdownActiveSubmitAndDispatch(t *testing.T) {
	f, p, store, path := shutdownFixture(t, 2*time.Second, nil)
	started := make(chan struct{})
	returned := make(chan struct{})
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		close(started)
		<-ctx.Done()
		defer close(returned)
		return nil, ctx.Err()
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	dispatchEntered, releaseDispatch := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(releaseDispatch) }) }
	t.Cleanup(unblock)
	f.s.onDispatchDone = func(_, _ int64) { close(dispatchEntered); <-releaseDispatch }
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	result := make(chan error, 1)
	go func() {
		_, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 9702, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
		result <- err
	}()
	awaitShutdownSignal(t, started)
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.shutdown() }()
	awaitShutdownSignal(t, dispatchEntered)
	awaitShutdownSignal(t, f.s.lifecycle.done)
	if closed, _, _ := store.snapshot(); closed {
		t.Fatal("store closed before abandoned dispatch exited")
	}
	unblock()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	awaitShutdownSignal(t, returned)
	if err := <-result; err == nil {
		t.Fatal("shutdown reported successful RPC")
	}
	assertShutdownStoreClosed(t, store)
	loadShutdownRecord(t, path, JobRecord{JobID: 9702, State: JobCancelled, AttemptID: 1, WorkerID: "worker-a", TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
}

func TestCoordinatorShutdownDuringSave(t *testing.T) {
	for _, state := range []JobState{JobRunning, JobFailed} {
		t.Run(map[JobState]string{JobRunning: "Running publication", JobFailed: "P0 Failed commit wins"}[state], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			f, p, store, path := shutdownFixture(t, 2*time.Second, func(record JobRecord) {
				if record.JobID == 9703 && record.State == state {
					close(entered)
					<-release
				}
			})
			var calls atomic.Int32
			address := startMTLSBackend(t, f, testpki.Worker, func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return nil, status.Error(codes.Unavailable, "injected failure")
			})
			registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			result := make(chan error, 1)
			go func() {
				_, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 9703, TaskType: "hash", Payload: "preserved", TimeoutMs: 1234})
				result <- err
			}()
			awaitShutdownSignal(t, entered)
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.shutdown() }()
			awaitShutdownSignal(t, f.s.lifecycle.ctx.Done())
			if closed, _, _ := store.snapshot(); closed {
				t.Fatal("store closed during Save")
			}
			select {
			case err := <-shutdown:
				t.Fatalf("shutdown skipped blocked Save: %v", err)
			default:
			}
			unblock()
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			<-result
			wantState, wantAttempt := JobCancelled, int64(1)
			if state == JobFailed {
				wantState, wantAttempt = JobFailed, 2
				if calls.Load() != 2 {
					t.Fatal("P0 retry budget changed")
				}
			}
			assertShutdownStoreClosed(t, store)
			loadShutdownRecord(t, path, JobRecord{JobID: 9703, State: wantState, AttemptID: wantAttempt, WorkerID: "worker-a", TaskType: "hash", Payload: "preserved", TimeoutMs: 1234})
		})
	}
}

func TestCoordinatorShutdownRecoveryRepair(t *testing.T) {
	f, p, store, path := shutdownFixture(t, 2*time.Second, nil)
	record := JobRecord{JobID: 9704, State: JobRecovering, AttemptID: 7, WorkerID: "old", TaskType: "hash", Payload: "recovery", TimeoutMs: 1000}
	saveRecoveryRecord(t, store, record)
	started := make(chan struct{})
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if req.AttemptId != 8 {
			t.Error("recovery generation changed")
		}
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	awaitShutdownSignal(t, started)
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	assertShutdownStoreClosed(t, store)
	record.AttemptID, record.WorkerID = 8, "worker-a"
	loadShutdownRecord(t, path, record)
}

func TestCoordinatorShutdownBeforeP0Finalization(t *testing.T) {
	before := make(chan struct{})
	f, p, store, path := shutdownFixture(t, 2*time.Second, nil, func(f *mtlsCoordinator, _ *coordinatorProcess) {
		f.observer.hook = func(event recordedMetric) {
			if event.name == "finished" && event.source == metrics.SourceSubmit && f.observer.count("finished", "") == 2 {
				close(before)
				<-f.s.lifecycle.ctx.Done()
			}
		}
	})
	address := startMTLSBackend(t, f, testpki.Worker, func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return nil, status.Error(codes.Unavailable, "injected failure")
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 9707, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	}()
	awaitShutdownSignal(t, before)
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	awaitShutdownSignal(t, finished)
	assertShutdownStoreClosed(t, store)
	loadShutdownRecord(t, path, JobRecord{JobID: 9707, State: JobCancelled, AttemptID: 2, WorkerID: "worker-a", TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
}

func TestCoordinatorShutdownDuringVerification(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean after Verify returns", true: "incomplete while Verify blocks"}[timeout], func(t *testing.T) {
			budget := 2 * time.Second
			if timeout {
				budget = 75 * time.Millisecond
			}
			f, p, store, path := shutdownFixture(t, budget, nil)
			output, digest, verifier := preimageProofFixture(t)
			barrier := &acceptanceBarrierVerifier{verifier: verifier, entered: make(chan struct{}), release: make(chan struct{})}
			f.s.preimageVerifier = barrier
			var once sync.Once
			unblock := func() { once.Do(func() { close(barrier.release) }) }
			t.Cleanup(unblock)
			address := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				resp := successfulResult(req)
				resp.Output = output
				return resp, nil
			})
			registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			payload := preimagePublicPayload(digest)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				_, _ = client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 9706, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000})
			}()
			awaitShutdownSignal(t, barrier.entered)
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.shutdown() }()
			awaitShutdownSignal(t, f.s.lifecycle.ctx.Done())
			if timeout {
				if err := <-shutdown; !errors.Is(err, errCoordinatorShutdownIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Verify timeout=%v", err)
				}
			}
			if closed, _, _ := store.snapshot(); closed {
				t.Fatal("closed store while Verify still running")
			}
			unblock()
			if !timeout {
				if err := <-shutdown; err != nil {
					t.Fatal(err)
				}
			}
			awaitShutdownSignal(t, finished)
			awaitShutdownSignal(t, p.cleanupDone)
			want := preimageRecord(9706, 1, "worker-a", JobCancelled, payload)
			if timeout {
				if closed, after, _ := store.snapshot(); closed || after != 0 {
					t.Fatal("timed-out shutdown resumed cleanup")
				}
				assertRecoveryRecord(t, store, want)
			} else {
				assertShutdownStoreClosed(t, store)
				loadShutdownRecord(t, path, want)
			}
			if barrier.calls.Load() != 1 || f.s.jobAttempts[9706] != 1 {
				t.Fatal("shutdown retried or accepted late proof")
			}
		})
	}
}

func TestCoordinatorShutdownCleanupUsesOverallBudget(t *testing.T) {
	s := newTestCoordinator()
	p := newCoordinatorProcess(s, 40*time.Millisecond)
	var closed atomic.Bool
	p.closeStore = func() error { closed.Store(true); return nil }
	p.shutdownMetrics = func(ctx context.Context) error {
		if ctx.Err() != nil || s.lifecycle.ctx.Err() == nil {
			t.Error("cleanup context not independent")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	err := p.shutdown()
	if !errors.Is(err, errCoordinatorShutdownIncomplete) || !errors.Is(err, context.DeadlineExceeded) || closed.Load() {
		t.Fatalf("cleanup timeout=%v closed=%t", err, closed.Load())
	}
	awaitShutdownSignal(t, p.cleanupDone)
}

func TestCoordinatorShutdownTimeoutDoesNotCloseActiveStore(t *testing.T) {
	for _, name := range []string{"handler", "Save", "startup"} {
		t.Run(name, func(t *testing.T) {
			s := newTestCoordinator()
			p := newCoordinatorProcess(s, 40*time.Millisecond)
			var closed atomic.Bool
			p.closeStore = func() error { closed.Store(true); return nil }
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var returned chan struct{}
			switch name {
			case "handler":
				_, leave, ok := s.lifecycle.tryEnter(context.Background())
				if !ok {
					t.Fatal("admission denied")
				}
				returned = make(chan struct{})
				go func() { <-release; leave(); close(returned) }()
			case "Save":
				entered := make(chan struct{})
				inner := NewMemoryJobStore()
				s.jobStore = &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
					if record.State == JobRunning {
						close(entered)
						<-release
					}
					return inner.Save(record)
				}}
				ctx, leave, _ := s.lifecycle.tryEnter(context.Background())
				returned = make(chan struct{})
				// A registry entry is sufficient: publication blocks before dispatch.
				s.mu.Lock()
				s.workers = map[string]WorkerInfo{"worker": {ID: "worker", Address: "127.0.0.1:1", Status: WorkerAlive}}
				s.workerOrder = []string{"worker"}
				s.mu.Unlock()
				go func() {
					defer close(returned)
					defer leave()
					_, _ = s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9705, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
				}()
				awaitShutdownSignal(t, entered)
			case "startup":
				p.startupDone = release
			}
			err := p.shutdown()
			if !errors.Is(err, errCoordinatorShutdownIncomplete) || !errors.Is(err, context.DeadlineExceeded) || closed.Load() {
				t.Fatalf("timeout=%v closed=%t", err, closed.Load())
			}
			unblock()
			if returned != nil {
				awaitShutdownSignal(t, returned)
			}
			awaitShutdownSignal(t, p.cleanupDone)
			if closed.Load() {
				t.Fatal("timeout later resumed ordinary cleanup")
			}
		})
	}
}

func TestCoordinatorShutdownUnexpectedServeAndPartialStartup(t *testing.T) {
	for _, serving := range []bool{false, true} {
		s := newTestCoordinator()
		p := newCoordinatorProcess(s, time.Second)
		var closed atomic.Bool
		p.closeStore = func() error { closed.Store(true); return nil }
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if serving {
			_ = listener.Close()
			p.start(grpc.NewServer(), listener)
			if err := <-p.serveResult; err == nil {
				t.Fatal("expected Serve failure")
			}
		} else {
			p.listener = listener
		}
		if err := p.shutdown(); err != nil || !closed.Load() {
			t.Fatalf("cleanup=%v closed=%t", err, closed.Load())
		}
	}
}

func TestCoordinatorLivenessStopsOnCancellation(t *testing.T) {
	s := newTestCoordinator()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.runLivenessMonitor(ctx, time.Hour, 3*time.Second) }()
	cancel()
	awaitShutdownSignal(t, done)
	if !reflect.DeepEqual(s.workers, map[string]WorkerInfo(nil)) {
		t.Fatal("cancelled monitor changed registry")
	}
}
