package main

import "testing"

func TestJobStatePersistedValues(t *testing.T) {
	for _, tc := range []struct {
		state JobState
		value int
	}{
		{JobQueued, 0},
		{JobRunning, 1},
		{JobSucceeded, 2},
		{JobFailed, 3},
		{JobCancelled, 4},
		{JobRecovering, 5},
	} {
		if int(tc.state) != tc.value {
			t.Errorf("persisted state %v = %d, want %d", tc.state, tc.state, tc.value)
		}
	}
}

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
