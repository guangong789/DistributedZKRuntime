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

// persistSubmitCancellation runs while the submit session still owns JobID.
// initialAttempt is the memory generation before this session executed; only
// an attempt published afterward belongs to its cancellation finalization.
// Reload/check/Save uses the same local mutex ordering as publishAttempt and
// persistTerminalResult. Metrics are emitted by the caller after unlocking.
func (s *CoordinatorServer) persistSubmitCancellation(jobID, initialAttempt int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, owned := s.activeJobs[jobID]
	attempt := s.jobAttempts[jobID]
	if s.jobStore == nil || !owned || attempt <= initialAttempt {
		return false, nil
	}
	current, ok, err := s.jobStore.Load(jobID)
	if err != nil {
		return false, fmt.Errorf("load current job: %w", err)
	}
	if !ok || current.JobID != jobID || current.State != JobRunning || current.AttemptID != attempt {
		return false, nil
	}
	current.State = JobCancelled
	if err := s.jobStore.Save(current); err != nil {
		return false, fmt.Errorf("save cancelled job: %w", err)
	}
	return true, nil
}

// persistSubmitFailure closes only this submit session's published Running
// generation. The caller retains ownership through Save, as with cancellation.
// An expired lease must not prevent failure finalization. Cancellation is
// checked again after Load; once Save succeeds the terminal commit wins.
func (s *CoordinatorServer) persistSubmitFailure(ctx context.Context, jobID, initialAttempt, expectedAttempt int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, owned := s.activeJobs[jobID]
	if s.jobStore == nil || !owned || expectedAttempt <= initialAttempt || expectedAttempt <= 0 || s.jobAttempts[jobID] != expectedAttempt {
		return false, nil
	}
	current, ok, err := s.jobStore.Load(jobID)
	if err != nil {
		return false, fmt.Errorf("load current job: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !ok || current.JobID != jobID || current.State != JobRunning || current.AttemptID != expectedAttempt {
		return false, nil
	}
	current.State = JobFailed
	if err := s.jobStore.Save(current); err != nil {
		return false, fmt.Errorf("save failed job: %w", err)
	}
	return true, nil
}

// Called after store initialization, before any startup recovery writes or producers.
// Inspect both states even after finding a match; never parse legacy payloads.
func (s *CoordinatorServer) checkDisabledZKRecovery() error {
	unfinished := false
	for _, state := range []JobState{JobRunning, JobRecovering} {
		records, err := s.jobStore.ListByState(state)
		if err != nil {
			return fmt.Errorf("check unfinished ZK jobs: %w", err)
		}
		for _, record := range records {
			if record.TaskType == "zk_preimage_prove" {
				unfinished = true
			}
		}
	}
	if unfinished {
		return errors.New("ZK is disabled but unfinished ZK jobs exist; restart with ZK enabled")
	}
	return nil
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
		spec, err := validateJobSpec(jobSpecFromRecord(record))
		if err != nil {
			// Keep unsafe legacy rows untouched for explicit operator correction.
			// Neither redispatch nor resave their original potentially private JSON.
			fmt.Printf("startup recovery rejected job %d: invalid job specification\n", record.JobID)
			continue
		}
		record.Payload = spec.Payload
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
	_, err := s.retryRecoveringJobResult(ctx, record)
	return err
}

// recoveryExecution records provenance before releasing ownership. A nil error
// alone does not imply execution: a stale snapshot is deliberately skipped.
type recoveryExecution struct {
	executed     bool
	terminal     bool
	repairFailed bool
	generation   int64
	specHash     [32]byte
}

func (s *CoordinatorServer) retryRecoveringJobResult(ctx context.Context, record JobRecord) (result recoveryExecution, err error) {
	result.generation = record.AttemptID
	result.specHash = recoverySpecHash(record)
	if record.State != JobRecovering {
		return result, fmt.Errorf(
			"job %d is not recovering",
			record.JobID,
		)
	}

	if !s.claimJob(record.JobID) {
		return result, fmt.Errorf(
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
			return result, err
		}
		if !ok || current.State != JobRecovering || current.AttemptID != record.AttemptID {
			return result, nil
		}
		record = current
	}

	result.specHash = recoverySpecHash(record)
	spec := jobSpecFromRecord(record)

	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceRecovery, time.Since(start)) }()
	s.mu.Lock()
	initialAttempt := s.jobAttempts[record.JobID]
	s.mu.Unlock()

	result.executed = true
	response, err := s.executeJobFromSource(ctx, spec, metrics.SourceRecovery)
	result.terminal = err == nil
	if result.terminal {
		result.generation = response.AttemptId
	}
	if err != nil {
		observer.RecoveryExecutionFailed(recoveryFailureReason(err))
		s.mu.Lock()
		failedAttempt := s.jobAttempts[record.JobID]
		s.mu.Unlock()
		// Repair only a generation published by this recovery execution.
		// Keep ownership until the repair finishes, including during shutdown.
		if failedAttempt > initialAttempt {
			result.generation = failedAttempt
			if repairErr := s.restoreRecoveringJob(record.JobID, failedAttempt); repairErr != nil {
				result.repairFailed = true
				return result, errors.Join(err, fmt.Errorf("restore recovering job %d attempt %d: %w",
					record.JobID, failedAttempt, repairErr))
			}
		}
	}
	return result, err
}
