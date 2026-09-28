package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func assertSQLiteJobRecord(t *testing.T, store *SQLiteJobStore, want JobRecord) {
	t.Helper()
	got, ok, err := store.Load(want.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("Load(%d): got=%+v found=%v error=%v, want %+v", want.JobID, got, ok, err, want)
	}
}

func assertSQLiteStateRecord(t *testing.T, store *SQLiteJobStore, want JobRecord) {
	t.Helper()
	got, err := store.ListByState(want.State)
	if err != nil {
		t.Fatalf("ListByState(%d): %v", want.State, err)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("ListByState(%d): got=%+v, want [%+v]", want.State, got, want)
	}
}

func TestSQLiteJobStoreFullRecordRoundTripAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	want := JobRecord{
		JobID:     101,
		State:     JobRunning,
		AttemptID: 7,
		WorkerID:  "worker-a",
		TaskType:  "hash",
		Payload:   `{"input":"hello"}`,
		TimeoutMs: 1250,
	}

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

		if err := storeA.Save(want); err != nil {
			t.Fatalf("save full job record: %v", err)
		}
		assertSQLiteJobRecord(t, storeA, want)
		assertSQLiteStateRecord(t, storeA, want)
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
	assertSQLiteJobRecord(t, storeB, want)
	assertSQLiteStateRecord(t, storeB, want)
}

func TestSQLiteJobStoreMigratesLegacySchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")

	// Create the four-column table used before task specifications were stored.
	func() {
		legacyDB, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open legacy SQLite database: %v", err)
		}
		defer func() {
			if err := legacyDB.Close(); err != nil {
				t.Errorf("close legacy SQLite database: %v", err)
			}
		}()
		if _, err := legacyDB.Exec(`
			CREATE TABLE jobs (
				job_id INTEGER PRIMARY KEY,
				state INTEGER NOT NULL,
				attempt_id INTEGER NOT NULL,
				worker_id TEXT NOT NULL
			)
		`); err != nil {
			t.Fatalf("create legacy jobs table: %v", err)
		}
		// State 1 was Running and state 2 was Succeeded before migration.
		if _, err := legacyDB.Exec(`
			INSERT INTO jobs (job_id, state, attempt_id, worker_id)
			VALUES (100, 1, 7, 'worker-a'), (200, 2, 3, 'worker-b')
		`); err != nil {
			t.Fatalf("insert legacy jobs: %v", err)
		}
	}()

	legacyRunning := JobRecord{JobID: 100, State: JobRunning, AttemptID: 7, WorkerID: "worker-a"}
	legacySucceeded := JobRecord{JobID: 200, State: JobSucceeded, AttemptID: 3, WorkerID: "worker-b"}
	updatedRunning := legacyRunning
	updatedRunning.TaskType = "sleep"
	updatedRunning.Payload = `{"duration_ms":50}`
	updatedRunning.TimeoutMs = 500

	func() {
		storeA, err := NewSQLiteJobStore(dbPath)
		if err != nil {
			t.Fatalf("open and migrate legacy database: %v", err)
		}
		defer func() {
			if err := storeA.Close(); err != nil {
				t.Errorf("close migrated SQLite store: %v", err)
			}
		}()

		// Full-struct comparison also checks the new columns' empty/zero defaults.
		assertSQLiteJobRecord(t, storeA, legacyRunning)
		assertSQLiteJobRecord(t, storeA, legacySucceeded)
		assertSQLiteStateRecord(t, storeA, legacyRunning)
		assertSQLiteStateRecord(t, storeA, legacySucceeded)

		if err := storeA.Save(updatedRunning); err != nil {
			t.Fatalf("save task specification into migrated database: %v", err)
		}
		assertSQLiteJobRecord(t, storeA, updatedRunning)
		assertSQLiteStateRecord(t, storeA, updatedRunning)
	}()

	// Initialization must be safe to repeat after the columns already exist.
	storeB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("reopen migrated database: %v", err)
	}
	defer func() {
		if err := storeB.Close(); err != nil {
			t.Errorf("close reopened migrated database: %v", err)
		}
	}()
	assertSQLiteJobRecord(t, storeB, updatedRunning)
	assertSQLiteJobRecord(t, storeB, legacySucceeded)
	assertSQLiteStateRecord(t, storeB, updatedRunning)
	assertSQLiteStateRecord(t, storeB, legacySucceeded)
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

func TestSQLiteJobStoreListByStateAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	records := listByStateRecords()

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

		for _, record := range records {
			if err := storeA.Save(record); err != nil {
				t.Fatalf("save job %d: %v", record.JobID, err)
			}
		}
		assertListedRecords(t, storeA, JobRunning, records[0], records[2])
		assertListedRecords(t, storeA, JobSucceeded, records[1])
		assertListedRecords(t, storeA, JobFailed, records[3])
		assertListedRecords(t, storeA, JobCancelled, records[4])
		assertListedRecords(t, storeA, JobQueued)
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

	assertListedRecords(t, storeB, JobRunning, records[0], records[2])
	assertListedRecords(t, storeB, JobQueued)
}

func TestStartAttemptContinuesAfterSQLiteReopen(t *testing.T) {
	const jobID int64 = 501
	const taskType, payload, timeoutMs = "hash", "hello", int64(1000)
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
			attemptID, err := coordinatorA.startAttempt(jobID, "worker-before-restart", time.Minute, taskType, payload, timeoutMs)
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
			record.AttemptID != 3 || record.WorkerID != "worker-before-restart" ||
			record.TaskType != taskType || record.Payload != payload || record.TimeoutMs != timeoutMs {
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

	attemptID, err := coordinatorB.startAttempt(jobID, "worker-after-restart", time.Minute, taskType, payload, timeoutMs)
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
		record.AttemptID != 4 || record.WorkerID != "worker-after-restart" ||
		record.TaskType != taskType || record.Payload != payload || record.TimeoutMs != timeoutMs {
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
