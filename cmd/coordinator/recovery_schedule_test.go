package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This clock controls only scheduler time. Existing RPC/lease clocks are real.
// Reset acknowledgments let tests know a pass has completed before advancing.
type manualRecoveryClock struct {
	mu      sync.Mutex
	nowTime time.Time
	due     time.Time
	active  bool
	ticks   chan time.Time
	resets  chan time.Duration
}

func newManualRecoveryClock() *manualRecoveryClock {
	return &manualRecoveryClock{nowTime: time.Unix(1000, 0), ticks: make(chan time.Time, 1), resets: make(chan time.Duration, 64)}
}
func (c *manualRecoveryClock) now() time.Time            { c.mu.Lock(); defer c.mu.Unlock(); return c.nowTime }
func (c *manualRecoveryClock) channel() <-chan time.Time { return c.ticks }
func (c *manualRecoveryClock) reset(d time.Duration) {
	c.mu.Lock()
	c.due, c.active = c.nowTime.Add(d), true
	select {
	case <-c.ticks:
	default:
	}
	if d <= 0 {
		c.ticks <- c.nowTime
		c.active = false
	}
	c.mu.Unlock()
	c.resets <- d
}
func (c *manualRecoveryClock) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	select {
	case <-c.ticks:
	default:
	}
}
func (c *manualRecoveryClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nowTime = c.nowTime.Add(d)
	if c.active && !c.nowTime.Before(c.due) {
		c.ticks <- c.nowTime
		c.active = false
	}
}
func testRecoverySchedule(c *manualRecoveryClock) *recoverySchedule {
	q := newRecoverySchedule()
	q.now, q.sample = c.now, func(n int64) int64 { return n - 1 }
	return q
}
func waitRecoveryReset(t *testing.T, c *manualRecoveryClock, want time.Duration) {
	t.Helper()
	select {
	case got := <-c.resets:
		if got != want {
			t.Fatalf("retry wait=%v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not arm retry timer")
	}
}
func startManualRecovery(t *testing.T, s *CoordinatorServer, q *recoverySchedule, c *manualRecoveryClock) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.runRecoveryScheduler(ctx, q, c) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}
func waitRecoverySignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery signal missing")
	}
}

func TestRecoveryBackoffBoundsAndSaturation(t *testing.T) {
	for _, tc := range []struct {
		failures uint32
		cap      time.Duration
	}{
		{0, time.Second}, {1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second},
		{4, 8 * time.Second}, {5, 16 * time.Second}, {6, 30 * time.Second}, {7, 30 * time.Second}, {^uint32(0), 30 * time.Second},
	} {
		for _, sample := range []func(int64) int64{func(int64) int64 { return 0 }, func(n int64) int64 { return n / 2 }, func(n int64) int64 { return n - 1 }} {
			delay := recoveryBackoff(tc.failures, sample)
			if delay <= 0 || delay < tc.cap/2 || delay > tc.cap {
				t.Fatalf("failure=%d delay=%v cap=%v", tc.failures, delay, tc.cap)
			}
		}
		if recoveryBackoff(tc.failures, func(int64) int64 { return 0 }) != tc.cap/2 || recoveryBackoff(tc.failures, func(n int64) int64 { return n - 1 }) != tc.cap {
			t.Fatal("jitter endpoints not inclusive")
		}
	}
	if incrementRecoveryFailures(0) != 1 || incrementRecoveryFailures(^uint32(0)) != ^uint32(0) {
		t.Fatal("failure counter did not saturate")
	}
}

func TestRecoverySchedulerBusyWorkerBecomesAvailable(t *testing.T) {
	fixture := newIntegrationZKFixture(t)
	store := newWorkerFailureStore(t)
	record := preimageRecord(9800, 7, "old-worker", JobRecovering, fixture.payload)
	saveRecoveryRecord(t, store, record)
	observer := &recordingMetrics{}
	terminal := make(chan struct{}, 1)
	var accepted atomic.Bool
	observer.hook = func(e recordedMetric) {
		if e.name == "terminal" {
			accepted.Store(true)
		}
		// Terminal metrics precede ownership release and scheduler cleanup.
		// Wait for the entire pass before asserting the schedule is empty.
		if e.name == "pass" && accepted.Swap(false) {
			terminal <- struct{}{}
		}
	}
	s := &CoordinatorServer{jobStore: store, preimageVerifier: fixture.verifier, metrics: observer, recoveryReady: make(chan struct{}, 1)}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy capacity")
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(limiter.Release) })
	var calls atomic.Int32
	proving := &admissionProvingMetrics{}
	registerTestWorker(t, s, "worker", limitedPreimageWorker(fixture, limiter, proving, &calls))
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	stop := startManualRecovery(t, s, q, c)
	waitRecoveryReset(t, c, time.Second)
	want := record
	want.AttemptID, want.WorkerID = 9, "worker"
	assertRecoveryRecord(t, store, want)
	if calls.Load() != 2 || observer.count("recovery_failed", "") != 1 || observer.count("started", "") != 2 {
		t.Fatal("session did not contain exactly two failed attempts")
	}
	// Registrations and real Dead->Alive notifications share the cooldown gate.
	for i := 0; i < 3; i++ {
		worker, _, _ := coordinatorWorkerSnapshot(s, "worker")
		if _, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: worker.ID, Address: worker.Address}); err != nil {
			t.Fatal(err)
		}
		waitRecoveryReset(t, c, time.Second)
		s.mu.Lock()
		worker = s.workers["worker"]
		worker.Status = WorkerDead
		s.workers[worker.ID] = worker
		s.mu.Unlock()
		if _, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: worker.ID}); err != nil {
			t.Fatal(err)
		}
		waitRecoveryReset(t, c, time.Second)
	}
	release.Do(limiter.Release) // Stays Alive; releasing capacity sends no event.
	c.advance(time.Second - time.Nanosecond)
	s.notifyRecoveryReady()
	waitRecoveryReset(t, c, time.Nanosecond)
	if calls.Load() != 2 || proving.samples.Load() != 0 {
		t.Fatal("notification bypassed cooldown")
	}
	c.advance(time.Nanosecond)
	waitRecoverySignal(t, terminal)
	stop()
	want.State, want.AttemptID = JobSucceeded, 10
	assertRecoveryRecord(t, store, want)
	if calls.Load() != 3 || proving.samples.Load() != 1 || observer.count("verification", "") != 1 || observer.count("started", "") != 3 || observer.count("finished", "") != 3 || observer.count("submitted", "") != 0 || observer.count("terminal", "") != 1 {
		t.Fatalf("incorrect execution metrics: %+v", observer.snapshot())
	}
	if len(q.entries) != 0 || !q.deadline().IsZero() {
		t.Fatal("terminal job retained a retry")
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
	running8, running9 := record, record
	running8.State, running8.AttemptID, running8.WorkerID = JobRunning, 8, "worker"
	running9.State, running9.AttemptID, running9.WorkerID = JobRunning, 9, "worker"
	repaired := running9
	repaired.State = JobRecovering
	running10 := want
	running10.State = JobRunning
	assertWorkerFailureSaves(t, store, []JobRecord{record, running8, running9, repaired, running10, want})
}

func TestRecoverySchedulerCountsSessionsAndCompletionTime(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	s := newTestCoordinator()
	record := recoveringTestRecord()
	saveRecoveryRecord(t, s.jobStore, record)
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		c.advance(2 * time.Second) // execution time must not consume cooldown
		return nil, status.Error(codes.Unavailable, "injected")
	})
	for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		e := q.entries[record.JobID]
		if e.failures != uint32(i+1) || e.generation != 7+2*int64(i+1) || !e.nextRetry.Equal(c.now().Add(delay)) || calls.Load() != 2*int32(i+1) {
			t.Fatalf("session %d: entry=%+v calls=%d", i, e, calls.Load())
		}
		if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2*int32(i+1) {
			t.Fatal("early pass dispatched")
		}
		c.advance(delay)
	}
}

func TestRecoverySchedulerCoolingJobDoesNotBlockOthers(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	inner := NewMemoryJobStore()
	a := recoveringTestRecord()
	b, d := a, a
	b.JobID++
	d.JobID += 2
	for _, r := range []JobRecord{a, b, d} {
		saveRecoveryRecord(t, inner, r)
	}
	s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, list: func(JobState) ([]JobRecord, error) { return []JobRecord{a, b, d, b}, nil }}}
	q.entries[a.JobID] = retryEntry{generation: a.AttemptID, specHash: recoverySpecHash(a), failures: 3, nextRetry: c.now().Add(30 * time.Second)}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if r.JobId == a.JobID {
			t.Error("cooling job executed")
		}
		calls.Add(1)
		return successfulResult(r), nil
	})
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, inner, a)
	for _, r := range []JobRecord{b, d} {
		r.State, r.AttemptID, r.WorkerID = JobSucceeded, 8, "worker"
		assertRecoveryRecord(t, inner, r)
	}
	if calls.Load() != 2 || len(q.entries) != 1 {
		t.Fatal("cooldown blocked others or duplicate snapshot executed twice")
	}
}

func TestRecoverySchedulerNoWorkerAndOwnershipRecheck(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(map[bool]string{false: "no worker", true: "ownership held"}[owned], func(t *testing.T) {
			c := newManualRecoveryClock()
			q := testRecoverySchedule(c)
			observer := &recordingMetrics{}
			s := newTestCoordinator()
			s.metrics = observer
			r := recoveringTestRecord()
			saveRecoveryRecord(t, s.jobStore, r)
			if owned {
				if !s.claimJob(r.JobID) {
					t.Fatal("claim failed")
				}
				t.Cleanup(func() { s.releaseJob(r.JobID) })
			}
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			e := q.entries[r.JobID]
			wantFailures := uint32(1)
			if owned {
				wantFailures = 0
			}
			if e.failures != wantFailures || !e.nextRetry.Equal(c.now().Add(time.Second)) || len(s.jobAttempts) != 0 || len(s.leases) != 0 || observer.count("started", "") != 0 || observer.count("recovery_failed", "") != int(wantFailures) {
				t.Fatalf("entry=%+v metrics=%+v", e, observer.snapshot())
			}
			s.releaseJob(r.JobID)
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return successfulResult(r), nil
			})
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			assertRecoveryRecord(t, s.jobStore, r)
			c.advance(time.Second)
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			r.State, r.AttemptID, r.WorkerID = JobSucceeded, 8, "worker"
			assertRecoveryRecord(t, s.jobStore, r)
			if len(q.entries) != 0 {
				t.Fatal("completed job still scheduled")
			}
		})
	}
}

func TestRecoverySchedulerReconcilesReplacedRecords(t *testing.T) {
	for _, kind := range []string{"succeeded", "failed", "cancelled", "running", "deleted", "new generation", "new spec"} {
		t.Run(kind, func(t *testing.T) {
			c := newManualRecoveryClock()
			q := testRecoverySchedule(c)
			s := newTestCoordinator()
			old := recoveringTestRecord()
			r := old
			q.entries[r.JobID] = retryEntry{generation: 7, specHash: recoverySpecHash(old), failures: 5, nextRetry: c.now().Add(30 * time.Second)}
			switch kind {
			case "succeeded":
				r.State = JobSucceeded
			case "failed":
				r.State = JobFailed
			case "cancelled":
				r.State = JobCancelled
			case "running":
				r.State = JobRunning
				r.AttemptID = 20
			case "new generation":
				r.AttemptID = 20
			case "new spec":
				r.Payload = "replacement"
			}
			if kind != "deleted" {
				saveRecoveryRecord(t, s.jobStore, r)
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if req.Payload != r.Payload || req.AttemptId <= r.AttemptID {
					t.Error("executed stale snapshot")
				}
				return nil, status.Error(codes.Unavailable, "injected")
			})
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			if kind == "new generation" || kind == "new spec" {
				e := q.entries[r.JobID]
				if calls.Load() != 2 || e.failures != 1 || e.generation != r.AttemptID+2 || !e.nextRetry.Equal(c.now().Add(time.Second)) {
					t.Fatalf("new submission inherited old schedule: %+v", e)
				}
			} else if calls.Load() != 0 || len(q.entries) != 0 || len(s.jobAttempts) != 0 {
				t.Fatal("obsolete job scheduled")
			}
		})
	}
}

func TestRecoverySchedulerScanFailureTimer(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	// An overdue entry must not override scan cooldown, even with an empty store.
	q.entries[99] = retryEntry{nextRetry: c.now().Add(-time.Second)}
	var scans atomic.Int32
	observer := &recordingMetrics{}
	passes := make(chan struct{}, 8)
	observer.hook = func(e recordedMetric) {
		if e.name == "pass" {
			passes <- struct{}{}
		}
	}
	s := &CoordinatorServer{metrics: observer, recoveryReady: make(chan struct{}, 1), jobStore: &recoveryPassStore{list: func(JobState) ([]JobRecord, error) {
		if scans.Add(1) <= 2 {
			return nil, errors.New("temporary scan failure")
		}
		return nil, nil
	}}}
	stop := startManualRecovery(t, s, q, c)
	waitRecoveryReset(t, c, time.Second)
	waitRecoverySignal(t, passes)
	for i := 0; i < 3; i++ {
		s.notifyRecoveryReady()
		waitRecoveryReset(t, c, time.Second)
	}
	if scans.Load() != 1 {
		t.Fatal("readiness bypassed scan cooldown")
	}
	c.advance(time.Second)
	waitRecoveryReset(t, c, 2*time.Second)
	waitRecoverySignal(t, passes)
	if scans.Load() != 2 {
		t.Fatal("timer did not retry first scan")
	}
	s.notifyRecoveryReady()
	waitRecoveryReset(t, c, 2*time.Second)
	c.advance(2 * time.Second)
	waitRecoverySignal(t, passes)
	stop()
	if scans.Load() != 3 || len(q.entries) != 0 || !q.scan.nextRetry.IsZero() || observer.count("started", "") != 0 || observer.count("execution", "") != 0 || observer.count("pass", "") != 3 {
		t.Fatal("scan retry fabricated execution or retained stale deadlines")
	}
}

func TestRecoverySchedulerRepairFailureDoesNotRetryRunning(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	inner := NewMemoryJobStore()
	r := recoveringTestRecord()
	saveRecoveryRecord(t, inner, r)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{metrics: observer, jobStore: &recoveryPassStore{JobStore: inner, save: func(r JobRecord) error {
		if r.State == JobRecovering {
			return errors.New("repair failed")
		}
		return inner.Save(r)
	}}}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return nil, status.Error(codes.Unavailable, "injected")
	})
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	r.State, r.AttemptID, r.WorkerID = JobRunning, 9, "worker"
	assertRecoveryRecord(t, inner, r)
	if len(q.entries) != 0 || !q.deadline().IsZero() || observer.count("repair_failed", "") != 1 {
		t.Fatal("failed repair scheduled stranded Running")
	}
	c.advance(time.Hour)
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("stranded Running retried")
	}
	assertRecoveryOwnershipReleased(t, s, r.JobID)
}

func TestRecoverySchedulerShutdownLongWait(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	s := newTestCoordinator()
	r := recoveringTestRecord()
	saveRecoveryRecord(t, s.jobStore, r)
	q.entries[r.JobID] = retryEntry{generation: r.AttemptID, specHash: recoverySpecHash(r), failures: 5, nextRetry: c.now()}
	stop := startManualRecovery(t, s, q, c)
	waitRecoveryReset(t, c, 30*time.Second)
	stop()
	c.mu.Lock()
	active := c.active
	c.mu.Unlock()
	if active || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("shutdown left timer/phantom attempts")
	}
	c.advance(time.Hour)
}

func TestRecoverySchedulerRestartResetsCooldownNotGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	// A and its store are scoped away; only the SQLite file survives.
	func() {
		store, err := NewSQLiteJobStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		a := &CoordinatorServer{jobStore: store}
		if n, err := a.startAttempt(9810, "old-worker", time.Second, "hash", "saved-task", 1500); err != nil || n != 1 {
			t.Fatalf("attempt=%d error=%v", n, err)
		}
		if err := a.recoverRunningJobs(); err != nil {
			t.Fatal(err)
		}
		if err := a.runScheduledRecoveryPass(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		if q.entries[9810].failures != 1 {
			t.Fatal("no-worker session not scheduled")
		}
	}()
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	b := &CoordinatorServer{jobStore: store, recoveryReady: make(chan struct{}, 1)}
	if err := b.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	registerTestWorker(t, b, "new-worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if r.AttemptId != 2 || r.TaskType != "hash" || r.Payload != "saved-task" || r.TimeoutMs != 1500 {
			t.Errorf("lost generation/spec: %v", r)
		}
		return successfulResult(r), nil
	})
	fresh := testRecoverySchedule(c)
	if err := b.runScheduledRecoveryPass(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, store, JobRecord{JobID: 9810, State: JobSucceeded, AttemptID: 2, WorkerID: "new-worker", TaskType: "hash", Payload: "saved-task", TimeoutMs: 1500})
	if calls.Load() != 1 || len(fresh.entries) != 0 {
		t.Fatal("new scheduler inherited old cooldown")
	}
}

func TestRecoverySchedulerSkippedAndReadErrors(t *testing.T) {
	for _, scenario := range []string{"stale under ownership", "pass load failure", "ownership load failure"} {
		t.Run(scenario, func(t *testing.T) {
			c := newManualRecoveryClock()
			q := testRecoverySchedule(c)
			inner := NewMemoryJobStore()
			r := recoveringTestRecord()
			saveRecoveryRecord(t, inner, r)
			observer := &recordingMetrics{}
			loads := 0
			store := &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) {
				loads++
				if (scenario == "pass load failure" && loads == 1) || (scenario == "ownership load failure" && loads == 2) {
					return JobRecord{}, false, errors.New("load unavailable")
				}
				if scenario == "stale under ownership" && loads == 2 {
					terminal := r
					terminal.State = JobSucceeded
					saveRecoveryRecord(t, inner, terminal)
				}
				return inner.Load(id)
			}}
			s := &CoordinatorServer{jobStore: store, metrics: observer}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 || observer.count("execution", "") != 0 || observer.count("recovery_failed", "") != 0 || observer.count("terminal", "") != 0 || len(s.jobAttempts) != 0 {
				t.Fatal("skip/read error was recorded as execution")
			}
			if scenario == "stale under ownership" {
				if len(q.entries) != 0 {
					t.Fatal("stale nil outcome retained schedule")
				}
			} else {
				e := q.entries[r.JobID]
				if e.failures != 0 || !e.nextRetry.Equal(c.now().Add(time.Second)) {
					t.Fatalf("read recheck=%+v", e)
				}
				c.advance(time.Second)
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 || len(q.entries) != 0 {
					t.Fatal("read error had no future opportunity")
				}
			}
			assertRecoveryOwnershipReleased(t, s, r.JobID)
		})
	}
}

func TestRecoverySchedulerSessionGenerationProvenance(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	store := NewMemoryJobStore()
	snapshot := recoveringTestRecord()
	q.entries[snapshot.JobID] = retryEntry{generation: 7, specHash: recoverySpecHash(snapshot), failures: 4, nextRetry: c.now()}
	// A different submission advances the durable record after recovery releases
	// ownership. Its generation must not inherit this session's fifth failure.
	newer := snapshot
	newer.AttemptID = 20
	saveRecoveryRecord(t, store, newer)
	result := recoveryExecution{executed: true, generation: 9, specHash: recoverySpecHash(snapshot)}
	if err := q.completed(store, snapshot, result, errors.New("session failed")); err != nil {
		t.Fatal(err)
	}
	e := q.entries[snapshot.JobID]
	if e.generation != 20 || e.failures != 0 || !e.nextRetry.Equal(c.now().Add(time.Second)) {
		t.Fatalf("adopted unrelated generation as own retry: %+v", e)
	}
}

func TestRecoverySchedulerSimultaneousTimerAndNotification(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	s := newTestCoordinator()
	s.recoveryReady = make(chan struct{}, 1)
	r := recoveringTestRecord()
	saveRecoveryRecord(t, s.jobStore, r)
	var calls atomic.Int32
	started, release, terminal := make(chan struct{}, 1), make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	observer := &recordingMetrics{}
	observer.hook = func(e recordedMetric) {
		if e.name == "terminal" {
			terminal <- struct{}{}
		}
	}
	s.metrics = observer
	registerTestWorker(t, s, "worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if calls.Add(1) <= 2 {
			return nil, status.Error(codes.ResourceExhausted, "busy")
		}
		started <- struct{}{}
		select {
		case <-release:
			return successfulResult(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	stop := startManualRecovery(t, s, q, c)
	waitRecoveryReset(t, c, time.Second)
	// Both sources are eligible at the same logical time. Whichever select wins,
	// the second signal cannot cause another execution or overlap this session.
	c.advance(time.Second)
	s.notifyRecoveryReady()
	waitRecoverySignal(t, started)
	for i := 0; i < 10; i++ {
		s.notifyRecoveryReady()
	}
	if calls.Load() != 3 {
		t.Fatal("overlapping recovery sessions")
	}
	once.Do(func() { close(release) })
	waitRecoverySignal(t, terminal)
	stop()
	if calls.Load() != 3 || observer.count("started", "") != 3 || observer.count("finished", "") != 3 || observer.count("terminal", "") != 1 {
		t.Fatalf("duplicate session from timer/event: %+v", observer.snapshot())
	}
}

func TestRecoverySchedulerNotificationDuringPassDiscoversNewJob(t *testing.T) {
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	s := newTestCoordinator()
	s.recoveryReady = make(chan struct{}, 1)
	first := recoveringTestRecord()
	second := first
	second.JobID++
	saveRecoveryRecord(t, s.jobStore, first)
	started, release, finished := make(chan struct{}, 1), make(chan struct{}), make(chan struct{}, 2)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls atomic.Int32
	observer := &recordingMetrics{}
	observer.hook = func(e recordedMetric) {
		if e.name == "terminal" {
			finished <- struct{}{}
		}
	}
	s.metrics = observer
	registerTestWorker(t, s, "worker", func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if r.JobId == first.JobID {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return successfulResult(r), nil
	})
	stop := startManualRecovery(t, s, q, c)
	waitRecoverySignal(t, started)
	saveRecoveryRecord(t, s.jobStore, second)
	worker, _, _ := coordinatorWorkerSnapshot(s, "worker")
	for i := 0; i < 5; i++ {
		if _, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: worker.ID, Address: worker.Address}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.recoveryReady) != 1 || calls.Load() != 1 {
		t.Fatal("pass notifications not coalesced")
	}
	once.Do(func() { close(release) })
	waitRecoverySignal(t, finished)
	waitRecoverySignal(t, finished)
	stop()
	if calls.Load() != 2 || observer.count("pass", "") != 2 || len(q.entries) != 0 {
		t.Fatalf("new job lost or duplicate pass: %+v", observer.snapshot())
	}
}

func TestRecoveryExecutionResultDistinguishesTerminalFromSkip(t *testing.T) {
	s := newTestCoordinator()
	r := recoveringTestRecord()
	saveRecoveryRecord(t, s.jobStore, r)
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(req), nil
	})
	accepted, err := s.retryRecoveringJobResult(context.Background(), r)
	if err != nil || !accepted.executed || !accepted.terminal || accepted.repairFailed || accepted.generation != 8 {
		t.Fatalf("accepted result=%+v error=%v", accepted, err)
	}
	skipped, err := s.retryRecoveringJobResult(context.Background(), r)
	if err != nil || skipped.executed || skipped.terminal || skipped.repairFailed {
		t.Fatalf("stale nil error counted as execution: result=%+v error=%v", skipped, err)
	}
	assertRecoveryOwnershipReleased(t, s, r.JobID)
}
