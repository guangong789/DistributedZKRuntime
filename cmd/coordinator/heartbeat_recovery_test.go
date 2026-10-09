package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func TestHeartbeatRevivalSignalsRecoveryOnce(t *testing.T) {
	old := WorkerInfo{ID: "worker", Address: "localhost:1234", LastSeen: time.Now().Add(-time.Hour), Status: WorkerDead}
	observer := &recordingMetrics{}
	s := &CoordinatorServer{workers: map[string]WorkerInfo{old.ID: old}, workerOrder: []string{old.ID},
		recoveryReady: make(chan struct{}, 1), metrics: observer}
	observer.hook = func(recordedMetric) {
		// Transition callbacks remain outside the registry mutex.
		coordinatorWorkerSnapshot(s, old.ID)
	}
	before := time.Now()
	resp, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: old.ID})
	worker, ok, order := coordinatorWorkerSnapshot(s, old.ID)
	if err != nil || resp == nil || !resp.Accepted || !ok || worker.Status != WorkerAlive ||
		worker.ID != old.ID || worker.Address != old.Address || worker.LastSeen.Before(before) ||
		len(order) != 1 || order[0] != old.ID {
		t.Fatalf("revival: response=%v error=%v worker=%+v order=%v", resp, err, worker, order)
	}
	select {
	case <-s.recoveryReady:
	default:
		t.Error("Dead->Alive heartbeat did not notify recovery")
	}
	for i := 0; i < 3; i++ {
		before = time.Now()
		resp, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: old.ID})
		updated, _, _ := coordinatorWorkerSnapshot(s, old.ID)
		if err != nil || !resp.Accepted || updated.LastSeen.Before(before) || updated.LastSeen.Before(worker.LastSeen) {
			t.Fatalf("ordinary heartbeat did not refresh LastSeen: response=%v error=%v worker=%+v", resp, err, updated)
		}
		worker = updated
		select {
		case <-s.recoveryReady:
			t.Error("Alive->Alive heartbeat sent a recovery notification")
		default:
		}
	}
	resp, err = s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "unknown"})
	_, exists, order := coordinatorWorkerSnapshot(s, "unknown")
	if err != nil || resp == nil || resp.Accepted || exists || len(order) != 1 {
		t.Fatalf("unknown heartbeat changed registry: response=%v error=%v", resp, err)
	}
	select {
	case <-s.recoveryReady:
		t.Error("unknown heartbeat notified recovery")
	default:
	}
	if observer.count("worker", "dead/alive/heartbeat") != 1 || observer.count("worker", "") != 1 {
		t.Fatalf("duplicate revival metrics: %+v", observer.snapshot())
	}
}

func TestHeartbeatRevivalConcurrentAndCoalesced(t *testing.T) {
	const heartbeats = 32
	for _, tc := range []struct {
		name     string
		capacity int
		pending  bool
	}{
		// Extra test buffer space makes duplicate signaling observable.
		{"concurrent transition", heartbeats, false},
		{"already pending", 1, true},
		{"nil notification channel", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &recordingMetrics{}
			s := &CoordinatorServer{workers: map[string]WorkerInfo{"worker": {
				ID: "worker", Address: "localhost:1234", Status: WorkerDead, LastSeen: time.Now().Add(-time.Hour),
			}}, workerOrder: []string{"worker"}, metrics: observer}
			if tc.capacity != 0 {
				s.recoveryReady = make(chan struct{}, tc.capacity)
			}
			if tc.pending {
				s.recoveryReady <- struct{}{}
			}
			start, done := make(chan struct{}), make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < heartbeats; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					resp, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "worker"})
					if err != nil || resp == nil || !resp.Accepted {
						t.Errorf("concurrent heartbeat: response=%v error=%v", resp, err)
					}
				}()
			}
			go func() { wg.Wait(); close(done) }()
			close(start)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("heartbeats blocked on readiness without a recovery consumer")
			}
			want := 1
			if tc.capacity == 0 {
				want = 0
			}
			if len(s.recoveryReady) != want || observer.count("worker", "dead/alive/heartbeat") != 1 ||
				observer.count("worker", "") != 1 {
				t.Fatalf("queued=%d transitions=%+v, want %d notification and one transition", len(s.recoveryReady), observer.snapshot(), want)
			}
		})
	}
}

func TestHeartbeatRevivalWakesRecoveryLoop(t *testing.T) {
	store := newWorkerFailureStore(t) // Real temporary SQLite with write history.
	record := recoveringTestRecord()
	saveRecoveryRecord(t, store, record)
	observer := &recordingMetrics{}
	passFinished := make(chan struct{}, 2)
	observer.hook = func(event recordedMetric) {
		if event.name == "pass" {
			passFinished <- struct{}{}
		}
	}
	s := &CoordinatorServer{jobStore: store, metrics: observer, recoveryReady: make(chan struct{}, 1)}
	workerStarted := make(chan int64, 1)
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Int32
	registerTestWorker(t, s, "revived-worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.TaskType != record.TaskType || req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Error("recovery lost the persisted task specification")
		}
		workerStarted <- req.AttemptId
		select {
		case <-releaseWorker:
			return successfulResult(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	// Consume registration's original signal before testing heartbeat revival.
	<-s.recoveryReady
	s.mu.Lock()
	worker := s.workers["revived-worker"]
	worker.LastSeen = time.Now().Add(-time.Hour)
	s.workers[worker.ID] = worker
	s.mu.Unlock()
	s.checkWorkerLiveness(time.Second)
	if err := s.runRecoveryPass(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-passFinished
	assertRecoveryRecord(t, store, record)
	if calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("dead worker was dispatched or an unscheduled attempt was created")
	}
	client := startTestCoordinator(t, s)
	waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWaiting()
	loopCtx, stopLoop := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	clock := newManualRecoveryClock()
	schedule := testRecoverySchedule(clock)
	go func() { s.runRecoveryScheduler(loopCtx, schedule, clock); close(loopDone) }()
	t.Cleanup(func() {
		stopLoop()
		releaseOnce.Do(func() { close(releaseWorker) })
		select {
		case <-loopDone:
		case <-time.After(5 * time.Second):
			t.Error("recovery loop did not stop")
		}
	})
	// Initial discovery sees the dead worker and starts a cooldown.
	waitRecoveryReset(t, clock, time.Second)
	waitRecoverySignal(t, passFinished)
	// Revival wakes a pass but cannot bypass that cooldown.
	resp, err := client.Heartbeat(waitCtx, &runtimepb.HeartbeatRequest{WorkerId: worker.ID})
	if err != nil || resp == nil || !resp.Accepted {
		t.Fatalf("revival heartbeat: response=%v error=%v", resp, err)
	}
	waitRecoveryReset(t, clock, time.Second)
	waitRecoverySignal(t, passFinished)
	if calls.Load() != 0 {
		t.Fatal("revival bypassed recovery cooldown")
	}
	clock.advance(time.Second)
	select {
	case attempt := <-workerStarted:
		if attempt != 8 {
			t.Fatalf("recovery attempt=%d, want 8", attempt)
		}
	case <-waitCtx.Done():
		t.Fatal("heartbeat revival did not wake the existing recovery loop")
	}
	for i := 0; i < 3; i++ {
		if resp, err := client.Heartbeat(waitCtx, &runtimepb.HeartbeatRequest{WorkerId: worker.ID}); err != nil || !resp.Accepted {
			t.Fatalf("alive heartbeat during blocked recovery: response=%v error=%v", resp, err)
		}
	}
	if len(s.recoveryReady) != 0 {
		t.Error("ordinary heartbeats queued another recovery pass")
	}
	releaseOnce.Do(func() { close(releaseWorker) })
	select {
	case <-passFinished:
	case <-waitCtx.Done():
		t.Fatal("revival recovery did not complete")
	}
	stopLoop()
	select {
	case <-loopDone:
	case <-waitCtx.Done():
		t.Fatal("recovery loop did not exit")
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, worker.ID
	assertRecoveryRecord(t, store, want)
	running := want
	running.State = JobRunning
	assertWorkerFailureSaves(t, store, []JobRecord{record, running, want})
	alive, exists, order := coordinatorWorkerSnapshot(s, worker.ID)
	if !exists || alive.Status != WorkerAlive || alive.Address != worker.Address ||
		len(order) != 1 || order[0] != worker.ID || calls.Load() != 1 || s.jobAttempts[record.JobID] != 8 {
		t.Fatalf("revival changed identity/order or duplicated execution: worker=%+v order=%v calls=%d", alive, order, calls.Load())
	}
	if observer.count("worker", "dead/alive/heartbeat") != 1 || observer.count("pass", "") != 4 {
		t.Fatalf("duplicate transitions/recovery passes: %+v", observer.snapshot())
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}
