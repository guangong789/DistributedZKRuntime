package main

import (
	"sync"
	"testing"
	"time"
)

func TestStartAttemptPersistsJobRecord(t *testing.T) {
	store := NewMemoryJobStore()
	s := &CoordinatorServer{jobStore: store}
	const taskType, payload, timeoutMs = "hash", "hello", int64(1000)

	assertAttempt := func(wantAttempt int64, workerID string) {
		t.Helper()

		attemptID, err := s.startAttempt(500, workerID, time.Minute, taskType, payload, timeoutMs)
		if err != nil {
			t.Fatalf("startAttempt failed: %v", err)
		}
		if attemptID != wantAttempt {
			t.Fatalf("attempt ID = %d, want %d", attemptID, wantAttempt)
		}

		record, ok, err := store.Load(500)
		if err != nil {
			t.Fatalf("load persisted job: %v", err)
		}
		if !ok {
			t.Fatal("persisted job record is missing")
		}
		if record.JobID != 500 ||
			record.AttemptID != attemptID ||
			record.State != JobRunning ||
			record.WorkerID != workerID ||
			record.TaskType != taskType ||
			record.Payload != payload ||
			record.TimeoutMs != timeoutMs {
			t.Fatalf("persisted job record is incorrect: %+v", record)
		}

		lease, ok := s.getLease(500)
		if !ok {
			t.Fatal("in-memory lease is missing")
		}
		if lease.JobID != record.JobID ||
			lease.AttemptID != record.AttemptID ||
			lease.WorkerID != record.WorkerID {
			t.Fatalf("record and lease diverged: record=%+v lease=%+v", record, lease)
		}
		if s.jobAttempts[500] != record.AttemptID {
			t.Fatalf(
				"record and in-memory attempt diverged: record=%+v attempt=%d",
				record,
				s.jobAttempts[500],
			)
		}
	}

	assertAttempt(1, "worker-1")
	assertAttempt(2, "worker-2")
}

func TestStartAttemptContinuesAfterRestart(t *testing.T) {
	store := NewMemoryJobStore()
	const taskType, payload, timeoutMs = "sleep", "250ms", int64(1000)

	first := &CoordinatorServer{
		jobAttempts: make(map[int64]int64),
		leases:      make(map[int64]JobLease),
		jobStore:    store,
	}

	for expected := int64(1); expected <= 3; expected++ {
		attemptID, err := first.startAttempt(
			500,
			"worker-1",
			5*time.Second,
			taskType,
			payload,
			timeoutMs,
		)
		if err != nil {
			t.Fatalf(
				"startAttempt failed: %v",
				err,
			)
		}

		if attemptID != expected {
			t.Fatalf(
				"expected attempt %d, got %d",
				expected,
				attemptID,
			)
		}
	}

	beforeRestart, ok, err := store.Load(500)
	if err != nil || !ok {
		t.Fatalf("load before restart: record=%+v found=%v error=%v", beforeRestart, ok, err)
	}
	if beforeRestart.JobID != 500 || beforeRestart.State != JobRunning ||
		beforeRestart.AttemptID != 3 || beforeRestart.WorkerID != "worker-1" ||
		beforeRestart.TaskType != taskType || beforeRestart.Payload != payload ||
		beforeRestart.TimeoutMs != timeoutMs {
		t.Fatalf("persisted record before restart: %+v", beforeRestart)
	}

	// The new coordinator starts with no attempt or lease state. Sharing the
	// in-memory store isolates generation recovery from SQLite I/O.
	second := &CoordinatorServer{
		jobAttempts: make(map[int64]int64),
		leases:      make(map[int64]JobLease),
		jobStore:    store,
	}
	if len(second.jobAttempts) != 0 || len(second.leases) != 0 {
		t.Fatal("restarted coordinator must begin with empty in-memory state")
	}

	attemptID, err := second.startAttempt(500, "worker-after-restart", 5*time.Second, taskType, payload, timeoutMs)
	if err != nil {
		t.Fatalf("start attempt after restart: %v", err)
	}
	if attemptID != 4 {
		t.Fatalf("attempt after restart = %d, want 4", attemptID)
	}

	record, ok, err := store.Load(500)
	if err != nil || !ok {
		t.Fatalf("load after restart: record=%+v found=%v error=%v", record, ok, err)
	}
	if record.JobID != 500 || record.State != JobRunning ||
		record.AttemptID != 4 || record.WorkerID != "worker-after-restart" ||
		record.TaskType != taskType || record.Payload != payload ||
		record.TimeoutMs != timeoutMs {
		t.Fatalf("persisted record after restart: %+v", record)
	}

	lease, ok := second.getLease(500)
	if !ok || lease.JobID != record.JobID ||
		lease.AttemptID != record.AttemptID || lease.WorkerID != record.WorkerID ||
		second.jobAttempts[500] != record.AttemptID {
		t.Fatalf("restart state diverged: record=%+v lease=%+v found=%v attempts=%v",
			record, lease, ok, second.jobAttempts)
	}
}

func TestStartAttemptUsesHigherOfMemoryAndStore(t *testing.T) {
	for _, tt := range []struct {
		name             string
		memoryAttempt    int64
		persistedAttempt int64
		want             int64
	}{
		{"memory ahead", 7, 3, 8},
		{"store ahead", 2, 9, 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMemoryJobStore()
			if err := store.Save(JobRecord{
				JobID:     500,
				State:     JobRunning,
				AttemptID: tt.persistedAttempt,
				WorkerID:  "previous-worker",
			}); err != nil {
				t.Fatalf("seed store: %v", err)
			}

			s := &CoordinatorServer{
				jobAttempts: map[int64]int64{500: tt.memoryAttempt},
				jobStore:    store,
			}
			attemptID, err := s.startAttempt(500, "next-worker", time.Minute, "hash", "hello", 1000)
			if err != nil {
				t.Fatalf("start attempt: %v", err)
			}
			if attemptID != tt.want {
				t.Fatalf("attempt ID = %d, want %d", attemptID, tt.want)
			}

			record, ok, err := store.Load(500)
			if err != nil || !ok {
				t.Fatalf("load updated record: record=%+v found=%v error=%v", record, ok, err)
			}
			lease, leaseOK := s.getLease(500)
			if record.JobID != 500 || record.State != JobRunning ||
				record.AttemptID != tt.want || record.WorkerID != "next-worker" ||
				record.TaskType != "hash" || record.Payload != "hello" || record.TimeoutMs != 1000 ||
				!leaseOK || lease.JobID != record.JobID ||
				lease.AttemptID != record.AttemptID || lease.WorkerID != record.WorkerID ||
				s.jobAttempts[500] != tt.want {
				t.Fatalf("attempt state diverged: record=%+v lease=%+v found=%v attempts=%v",
					record, lease, leaseOK, s.jobAttempts)
			}
		})
	}
}

func TestLeaseCreatedForAttempt(t *testing.T) {
	s := &CoordinatorServer{
		leases: make(map[int64]JobLease),
	}

	s.setLease(
		500,
		2,
		"worker-1",
		time.Second,
	)

	lease, ok := s.getLease(500)
	if !ok {
		t.Fatal("expected lease")
	}

	if lease.AttemptID != 2 {
		t.Fatalf(
			"expected attempt 2, got %d",
			lease.AttemptID,
		)
	}

	if lease.WorkerID != "worker-1" {
		t.Fatalf(
			"expected worker-1, got %s",
			lease.WorkerID,
		)
	}
}

func TestIsLeaseCurrent(t *testing.T) {
	now := time.Now()

	s := &CoordinatorServer{
		leases: map[int64]JobLease{
			500: {
				JobID:     500,
				AttemptID: 2,
				WorkerID:  "worker-1",
				ExpiresAt: now.Add(time.Second),
			},
		},
	}

	if !s.isLeaseCurrent(500, 2) {
		t.Fatal("expected current valid lease")
	}

	if s.isLeaseCurrent(500, 1) {
		t.Fatal("old attempt should not be current")
	}

	s.mu.Lock()
	lease := s.leases[500]
	lease.ExpiresAt = time.Now().Add(-time.Second)
	s.leases[500] = lease
	s.mu.Unlock()

	if s.isLeaseCurrent(500, 2) {
		t.Fatal("expired lease should not be current")
	}
}

func TestLeaseExpiry(t *testing.T) {
	now := time.Now()

	s := &CoordinatorServer{
		leases: map[int64]JobLease{
			500: {
				JobID:     500,
				AttemptID: 1,
				WorkerID:  "worker-1",
				ExpiresAt: now.Add(time.Second),
			},
		},
	}

	if s.leaseExpired(500, now) {
		t.Fatal("lease should not be expired yet")
	}

	if !s.leaseExpired(
		500,
		now.Add(2*time.Second),
	) {
		t.Fatal("lease should be expired")
	}
}

func TestConcurrentAttemptAllocation(t *testing.T) {
	s := newTestCoordinator()
	const count = 100
	type attemptResult struct {
		id  int64
		err error
	}
	results := make(chan attemptResult, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := s.startAttempt(500, "worker", time.Minute, "hash", "hello", 1000)
			results <- attemptResult{id: id, err: err}
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[int64]bool)
	for result := range results {
		if result.err != nil {
			t.Fatalf("start attempt: %v", result.err)
		}
		id := result.id
		if id < 1 || id > count || seen[id] {
			t.Fatalf("invalid or duplicate attempt: %d", id)
		}
		seen[id] = true
	}
	lease, ok := s.getLease(500)
	if !ok || lease.AttemptID != count || !s.isLeaseCurrent(500, count) || s.isLeaseCurrent(500, count-1) {
		t.Fatalf("latest attempt and lease diverged: %+v", lease)
	}
	next, err := s.startAttempt(501, "worker", time.Minute, "hash", "second-job", 1000)
	if err != nil {
		t.Fatalf("start attempt for second job: %v", err)
	}
	if next != 1 {
		t.Fatalf("attempt IDs are not per-job: %d", next)
	}
}

func TestLeaseExpiryBoundaryAndMissingLease(t *testing.T) {
	s := newTestCoordinator()
	if s.leaseExpired(1, time.Now()) || s.isLeaseCurrent(1, 1) {
		t.Fatal("missing lease reported expired or current")
	}
	s.setLease(1, 1, "worker", time.Minute)
	lease, _ := s.getLease(1)
	if !s.leaseExpired(1, lease.ExpiresAt) {
		t.Fatal("lease must be expired at its expiry time")
	}
}
