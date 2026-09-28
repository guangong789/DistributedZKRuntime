package main

import (
	"database/sql"
	"path/filepath"
	"testing"
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
