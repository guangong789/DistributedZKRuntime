package main

import (
	"fmt"
	"strings"
	"sync"
)

type JobState int

const (
	JobQueued JobState = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobCancelled
)

type JobRecord struct {
	JobID     int64
	State     JobState
	AttemptID int64
	WorkerID  string
}

type JobStore interface {
	Load(jobID int64) (JobRecord, bool, error)
	Save(record JobRecord) error
}

type MemoryJobStores struct {
	mu   sync.Mutex
	jobs map[int64]JobRecord
}

func NewMemoryJobStore() *MemoryJobStores {
	return &MemoryJobStores{
		jobs: make(map[int64]JobRecord),
	}
}

func (s *MemoryJobStores) Save(record JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[record.JobID] = record
	return nil
}

func (s *MemoryJobStores) Load(
	jobID int64,
) (JobRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.jobs[jobID]
	return record, ok, nil
}

func jobStateFromStatus(status string) (JobState, error) {
	switch strings.ToLower(status) {
	case "succeeded":
		return JobSucceeded, nil
	case "failed":
		return JobFailed, nil
	case "cancelled":
		return JobCancelled, nil
	default:
		return 0, fmt.Errorf("unknown job status %q", status)
	}
}
