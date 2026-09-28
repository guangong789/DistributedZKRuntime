package main

import (
	"context"
	"log"
)

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
// and leave persistence to executeJob; only listing failure or cancellation ends
// the pass early. A nil return means the scan completed, not that every job ran.
func (s *CoordinatorServer) runRecoveryPass(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.jobStore == nil {
		return nil
	}
	records, err := s.jobStore.ListByState(JobRecovering)
	if err != nil {
		return err
	}
	for _, snapshot := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, ok, err := s.jobStore.Load(snapshot.JobID)
		if err != nil {
			log.Printf("recovery load failed: job=%d err=%v", snapshot.JobID, err)
			continue
		}
		if !ok || current.State != JobRecovering || current.AttemptID != snapshot.AttemptID {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.retryRecoveringJob(ctx, current); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("recovery execution failed: job=%d err=%v", current.JobID, err)
		}
	}
	return ctx.Err()
}
