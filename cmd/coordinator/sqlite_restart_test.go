package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStartAttemptContinuesAfterSQLiteReopen(t *testing.T) {
	const jobID int64 = 501
	dbPath := filepath.Join(t.TempDir(), "jobs.db")

	// Scope the first coordinator and store so neither survives the reopen.
	func() {
		storeA, err := NewSQLiteJobStore(dbPath)
		if err != nil {
			t.Fatalf("open first SQLite store: %v", err)
		}
		defer func() {
			if err := storeA.Close(); err != nil {
				t.Errorf("close first SQLite store: %v", err)
			}
		}()

		coordinatorA := &CoordinatorServer{
			jobAttempts: make(map[int64]int64),
			leases:      make(map[int64]JobLease),
			jobStore:    storeA,
		}
		for want := int64(1); want <= 3; want++ {
			attemptID, err := coordinatorA.startAttempt(jobID, "worker-before-restart", time.Minute)
			if err != nil {
				t.Fatalf("start attempt %d before restart: %v", want, err)
			}
			if attemptID != want {
				t.Fatalf("attempt before restart = %d, want %d", attemptID, want)
			}
		}

		record, ok, err := storeA.Load(jobID)
		if err != nil || !ok {
			t.Fatalf("load before restart: record=%+v found=%v error=%v", record, ok, err)
		}
		if record.JobID != jobID || record.State != JobRunning ||
			record.AttemptID != 3 || record.WorkerID != "worker-before-restart" {
			t.Fatalf("record before restart = %+v", record)
		}
	}()

	storeB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite store: %v", err)
	}
	defer func() {
		if err := storeB.Close(); err != nil {
			t.Errorf("close reopened SQLite store: %v", err)
		}
	}()

	coordinatorB := &CoordinatorServer{
		jobAttempts: make(map[int64]int64),
		leases:      make(map[int64]JobLease),
		jobStore:    storeB,
	}
	if len(coordinatorB.jobAttempts) != 0 || len(coordinatorB.leases) != 0 {
		t.Fatal("restarted coordinator must begin with empty attempt and lease state")
	}

	before, ok, err := storeB.Load(jobID)
	if err != nil || !ok {
		t.Fatalf("load after reopen: record=%+v found=%v error=%v", before, ok, err)
	}
	if before.AttemptID != 3 {
		t.Fatalf("persisted attempt after reopen = %d, want 3", before.AttemptID)
	}

	attemptID, err := coordinatorB.startAttempt(jobID, "worker-after-restart", time.Minute)
	if err != nil {
		t.Fatalf("start attempt after reopen: %v", err)
	}
	if attemptID != 4 {
		t.Fatalf("attempt after reopen = %d, want 4", attemptID)
	}

	record, ok, err := storeB.Load(jobID)
	if err != nil || !ok {
		t.Fatalf("load after new attempt: record=%+v found=%v error=%v", record, ok, err)
	}
	if record.JobID != jobID || record.State != JobRunning ||
		record.AttemptID != 4 || record.WorkerID != "worker-after-restart" {
		t.Fatalf("record after restart = %+v", record)
	}
	lease, ok := coordinatorB.getLease(jobID)
	if !ok || lease.JobID != jobID || lease.AttemptID != 4 ||
		lease.WorkerID != "worker-after-restart" {
		t.Fatalf("lease after restart = %+v, found=%v", lease, ok)
	}
	if coordinatorB.jobAttempts[jobID] != 4 {
		t.Fatalf("in-memory attempt after restart = %d, want 4", coordinatorB.jobAttempts[jobID])
	}
}
