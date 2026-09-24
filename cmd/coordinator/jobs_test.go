package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMemoryJobStoreSaveAndLoad(t *testing.T) {
	store := NewMemoryJobStore()

	record := JobRecord{
		JobID:     500,
		State:     JobRunning,
		AttemptID: 7,
		WorkerID:  "worker-1",
	}

	if err := store.Save(record); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	got, ok, err := store.Load(500)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !ok {
		t.Fatal("expected job to exist")
	}

	if got != record {
		t.Fatalf("got %+v, want %+v", got, record)
	}
}

func TestMemoryJobStoreMissingJob(t *testing.T) {
	store := NewMemoryJobStore()

	_, ok, err := store.Load(999)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if ok {
		t.Fatal("expected job to be missing")
	}
}

func TestStartAttemptPersistsJobRecord(t *testing.T) {
	store := NewMemoryJobStore()
	s := &CoordinatorServer{jobStore: store}

	assertAttempt := func(wantAttempt int64, workerID string) {
		t.Helper()

		attemptID, err := s.startAttempt(500, workerID, time.Minute)
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
			record.WorkerID != workerID {
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
		beforeRestart.AttemptID != 3 || beforeRestart.WorkerID != "worker-1" {
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

	attemptID, err := second.startAttempt(500, "worker-after-restart", 5*time.Second)
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
		record.AttemptID != 4 || record.WorkerID != "worker-after-restart" {
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
			attemptID, err := s.startAttempt(500, "next-worker", time.Minute)
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
				!leaseOK || lease.JobID != record.JobID ||
				lease.AttemptID != record.AttemptID || lease.WorkerID != record.WorkerID ||
				s.jobAttempts[500] != tt.want {
				t.Fatalf("attempt state diverged: record=%+v lease=%+v found=%v attempts=%v",
					record, lease, leaseOK, s.jobAttempts)
			}
		})
	}
}

func TestSQLiteJobStorePersistsAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(
		t.TempDir(),
		"jobs.db",
	)

	store1, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteJobStore failed: %v", err)
	}

	record := JobRecord{
		JobID:     500,
		State:     JobRunning,
		AttemptID: 3,
		WorkerID:  "worker-1",
	}

	if err := store1.Save(record); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	store2, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite store failed: %v", err)
	}
	defer store2.Close()

	got, ok, err := store2.Load(500)
	if err != nil {
		t.Fatalf("Load after reopen failed: %v", err)
	}

	if !ok {
		t.Fatal("expected persisted job after reopening database")
	}

	if got != record {
		t.Fatalf(
			"got %+v after reopen, want %+v",
			got,
			record,
		)
	}
}

func TestSQLiteJobStoreUpdatesExistingJob(t *testing.T) {
	dbPath := filepath.Join(
		t.TempDir(),
		"jobs.db",
	)

	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteJobStore failed: %v", err)
	}
	defer store.Close()

	first := JobRecord{
		JobID:     500,
		State:     JobRunning,
		AttemptID: 1,
		WorkerID:  "worker-1",
	}

	if err := store.Save(first); err != nil {
		t.Fatalf("Save first record failed: %v", err)
	}

	second := JobRecord{
		JobID:     500,
		State:     JobRunning,
		AttemptID: 2,
		WorkerID:  "worker-2",
	}

	if err := store.Save(second); err != nil {
		t.Fatalf("Save second record failed: %v", err)
	}

	got, ok, err := store.Load(500)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !ok {
		t.Fatal("expected job to exist")
	}

	if got != second {
		t.Fatalf(
			"got %+v, want %+v",
			got,
			second,
		)
	}
}

func TestSQLiteJobStorePersistsTerminalStateAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")

	store1, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open store1: %v", err)
	}

	record := JobRecord{
		JobID:     500,
		State:     JobSucceeded,
		AttemptID: 3,
		WorkerID:  "worker-1",
	}

	if err := store1.Save(record); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("close store1: %v", err)
	}

	store2, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open store2: %v", err)
	}
	defer store2.Close()

	got, ok, err := store2.Load(500)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !ok {
		t.Fatal("expected persisted job")
	}

	if got != record {
		t.Fatalf("got %+v, want %+v", got, record)
	}
}
