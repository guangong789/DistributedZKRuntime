package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func recoveryFailureReason(err error) metrics.RecoveryFailureReason {
	switch {
	case errors.Is(err, context.Canceled):
		return metrics.FailureCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return metrics.FailureDeadline
	case status.Code(err) == codes.Unavailable:
		return metrics.FailureUnavailable
	case status.Code(err) == codes.Internal:
		return metrics.FailureInternal
	default:
		return metrics.FailureOther
	}
}

// restoreRecoveringJob repairs an unsuccessful recovery while the caller still
// owns claimJob. Ownership excludes other normal execution paths on this
// coordinator. Load/compare/Save is not an atomic conditional store update;
// multiple coordinators or independent store writers would require one.
func (s *CoordinatorServer) restoreRecoveringJob(jobID, failedAttempt int64) error {
	observer := metrics.Resolve(s.metrics)
	if s.jobStore == nil {
		return nil
	}
	current, ok, err := s.jobStore.Load(jobID)
	if err != nil {
		observer.RecoveryRepairFailed(metrics.RepairLoad)
		return fmt.Errorf("load current job: %w", err)
	}
	if !ok || current.State != JobRunning || current.AttemptID != failedAttempt {
		return nil
	}
	current.State = JobRecovering
	if err := s.jobStore.Save(current); err != nil {
		observer.RecoveryRepairFailed(metrics.RepairSave)
		return fmt.Errorf("save recovering job: %w", err)
	}
	observer.RecoveryTransition(metrics.RecoveryRepair)
	return nil
}

// runRecoveryLoop processes registrations serially until shutdown. Readiness
// signals coalesce while a pass is running.
func (s *CoordinatorServer) runRecoveryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-s.recoveryReady:
			if err := s.runRecoveryPass(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("recovery pass failed: %v", err)
			}
		}
	}
}

// runRecoveryPass processes one snapshot sequentially. Per-job errors are logged
// and leave persistence to retryRecoveringJob; only listing failure or cancellation ends
// the pass early. A nil return means the scan completed, not that every job ran.
func (s *CoordinatorServer) runRecoveryPass(ctx context.Context) error {
	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	outcome := metrics.PassCompleted
	defer func() { observer.RecoveryPass(outcome, time.Since(start)) }()
	if err := ctx.Err(); err != nil {
		outcome = metrics.PassCancelled
		return err
	}
	if s.jobStore == nil {
		outcome = metrics.PassSkipped
		return nil
	}
	records, err := s.jobStore.ListByState(JobRecovering)
	if err != nil {
		outcome = metrics.PassStoreError
		return err
	}
	for _, snapshot := range records {
		if err := ctx.Err(); err != nil {
			outcome = metrics.PassCancelled
			return err
		}
		current, ok, err := s.jobStore.Load(snapshot.JobID)
		if err != nil {
			outcome = metrics.PassCompletedWithErrors
			log.Printf("recovery load failed: job=%d err=%v", snapshot.JobID, err)
			continue
		}
		if !ok || current.State != JobRecovering || current.AttemptID != snapshot.AttemptID {
			continue
		}
		if err := ctx.Err(); err != nil {
			outcome = metrics.PassCancelled
			return err
		}
		if err := s.retryRecoveringJob(ctx, current); err != nil {
			if ctx.Err() != nil {
				outcome = metrics.PassCancelled
				// Preserve a repair failure joined with the cancellation error.
				return err
			}
			outcome = metrics.PassCompletedWithErrors
			log.Printf("recovery execution failed: job=%d err=%v", current.JobID, err)
		}
	}
	if ctx.Err() != nil {
		outcome = metrics.PassCancelled
	}
	return ctx.Err()
}
