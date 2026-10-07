package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
)

type JobState int

const (
	JobQueued JobState = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobCancelled
	JobRecovering // Keep existing states' persisted SQLite values unchanged.
)

type JobRecord struct {
	JobID     int64
	State     JobState
	AttemptID int64
	WorkerID  string

	TaskType  string
	Payload   string
	TimeoutMs int64
}

type JobStore interface {
	Load(jobID int64) (JobRecord, bool, error)
	Save(record JobRecord) error
	ListByState(state JobState) ([]JobRecord, error)
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

func (s *MemoryJobStores) ListByState(state JobState) ([]JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var records []JobRecord

	for _, record := range s.jobs {
		if record.State == state {
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *CoordinatorServer) recoverRunningJobs() error {
	observer := metrics.Resolve(s.metrics)
	if s.jobStore == nil {
		return nil
	}

	records, err := s.jobStore.ListByState(JobRunning)
	if err != nil {
		return err
	}

	for _, record := range records {
		record.State = JobRecovering

		if err := s.jobStore.Save(record); err != nil {
			return err
		}

		observer.RecoveryTransition(metrics.RecoveryStartup)
		fmt.Printf(
			"recovering job: id=%d attempt=%d worker=%s\n",
			record.JobID,
			record.AttemptID,
			record.WorkerID,
		)
	}

	return nil
}

func (s *CoordinatorServer) retryRecoveringJob(
	ctx context.Context,
	record JobRecord,
) error {
	if record.State != JobRecovering {
		return fmt.Errorf(
			"job %d is not recovering",
			record.JobID,
		)
	}

	if !s.claimJob(record.JobID) {
		return fmt.Errorf(
			"job %d is already active",
			record.JobID,
		)
	}
	defer s.releaseJob(record.JobID)

	// A submission may have completed between the recovery pass's reload and
	// this claim. Revalidate under ownership before executing the snapshot.
	if s.jobStore != nil {
		current, ok, err := s.jobStore.Load(record.JobID)
		if err != nil {
			return err
		}
		if !ok || current.State != JobRecovering || current.AttemptID != record.AttemptID {
			return nil
		}
		record = current
	}

	spec := jobSpecFromRecord(record)

	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceRecovery, time.Since(start)) }()
	s.mu.Lock()
	initialAttempt := s.jobAttempts[record.JobID]
	s.mu.Unlock()

	_, err := s.executeJobFromSource(ctx, spec, metrics.SourceRecovery)
	if err != nil {
		observer.RecoveryExecutionFailed(recoveryFailureReason(err))
		s.mu.Lock()
		failedAttempt := s.jobAttempts[record.JobID]
		s.mu.Unlock()
		// Repair only a generation published by this recovery execution.
		// Keep ownership until the repair finishes, including during shutdown.
		if failedAttempt > initialAttempt {
			if repairErr := s.restoreRecoveringJob(record.JobID, failedAttempt); repairErr != nil {
				return errors.Join(err, fmt.Errorf("restore recovering job %d attempt %d: %w",
					record.JobID, failedAttempt, repairErr))
			}
		}
	}
	return err
}
