package main

import (
	"path/filepath"
	"testing"
)

func listByStateRecords() []JobRecord {
	return []JobRecord{
		{JobID: 1, State: JobRunning, AttemptID: 2, WorkerID: "worker-1"},
		{JobID: 2, State: JobSucceeded, AttemptID: 3, WorkerID: "worker-2"},
		{JobID: 3, State: JobRunning, AttemptID: 4, WorkerID: "worker-3"},
		{JobID: 4, State: JobFailed, AttemptID: 5, WorkerID: "worker-4"},
		{JobID: 5, State: JobCancelled, AttemptID: 6, WorkerID: "worker-5"},
	}
}

func assertListedRecords(t *testing.T, store JobStore, state JobState, want ...JobRecord) {
	t.Helper()
	got, err := store.ListByState(state)
	if err != nil {
		t.Fatalf("ListByState(%d): %v", state, err)
	}
	if len(got) != len(want) {
		t.Fatalf("ListByState(%d) returned %d records, want %d: %+v", state, len(got), len(want), got)
	}

	wantByID := make(map[int64]JobRecord, len(want))
	for _, record := range want {
		wantByID[record.JobID] = record
	}
	for _, record := range got {
		expected, ok := wantByID[record.JobID]
		if !ok {
			t.Fatalf("ListByState(%d) returned unexpected or duplicate record: %+v", state, record)
		}
		if record != expected {
			t.Fatalf("ListByState(%d) returned %+v, want %+v", state, record, expected)
		}
		delete(wantByID, record.JobID)
	}
}

func TestMemoryJobStoreListByState(t *testing.T) {
	store := NewMemoryJobStore()
	records := listByStateRecords()
	for _, record := range records {
		if err := store.Save(record); err != nil {
			t.Fatalf("save job %d: %v", record.JobID, err)
		}
	}

	for _, tc := range []struct {
		name  string
		state JobState
		want  []JobRecord
	}{
		{name: "running", state: JobRunning, want: []JobRecord{records[0], records[2]}},
		{name: "succeeded", state: JobSucceeded, want: []JobRecord{records[1]}},
		{name: "failed", state: JobFailed, want: []JobRecord{records[3]}},
		{name: "cancelled", state: JobCancelled, want: []JobRecord{records[4]}},
		{name: "empty queued", state: JobQueued},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertListedRecords(t, store, tc.state, tc.want...)
		})
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
