package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"math/rand/v2"
	"time"
)

// retryEntry contains no task payload. The fingerprint detects replacement even
// at the same generation; a session's own generation advance retains failures.
type retryEntry struct {
	generation int64
	specHash   [32]byte
	failures   uint32
	nextRetry  time.Time
}

type recoverySchedule struct {
	entries map[int64]retryEntry
	scan    retryEntry
	now     func() time.Time
	sample  func(int64) int64
}

func newRecoverySchedule() *recoverySchedule {
	return &recoverySchedule{entries: make(map[int64]retryEntry), now: time.Now, sample: rand.Int64N}
}

func recoverySpecHash(r JobRecord) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("%q %q %d", r.TaskType, r.Payload, r.TimeoutMs)))
}

func incrementRecoveryFailures(n uint32) uint32 {
	if n < ^uint32(0) {
		n++
	}
	return n
}

// recoveryBackoff samples integer nanoseconds inclusively between half the
// capped delay and its full value. Saturating before doubling avoids overflow.
func recoveryBackoff(failures uint32, sample func(int64) int64) time.Duration {
	delay := time.Second
	for n := uint32(1); n < failures && delay < 30*time.Second; n++ {
		if delay >= 15*time.Second {
			delay = 30 * time.Second
		} else {
			delay *= 2
		}
	}
	half := delay / 2
	return half + time.Duration(sample(int64(delay-half)+1))
}

func (q *recoverySchedule) reconcile(records []JobRecord) {
	seen := make(map[int64]bool, len(records))
	for _, r := range records {
		seen[r.JobID] = true
		if e, ok := q.entries[r.JobID]; ok && (e.generation != r.AttemptID || e.specHash != recoverySpecHash(r)) {
			delete(q.entries, r.JobID)
		}
	}
	for id := range q.entries {
		if !seen[id] {
			delete(q.entries, id)
		}
	}
}

func (q *recoverySchedule) eligible(r JobRecord) bool {
	return !q.now().Before(q.entries[r.JobID].nextRetry)
}

// recheck does not count ownership contention/read failures as failed execution.
func (q *recoverySchedule) recheck(r JobRecord) {
	e := q.entries[r.JobID]
	hash := recoverySpecHash(r)
	if e.generation != r.AttemptID || e.specHash != hash {
		e = retryEntry{}
	}
	e.generation, e.specHash = r.AttemptID, hash
	e.nextRetry = q.now().Add(recoveryBackoff(1, q.sample))
	q.entries[r.JobID] = e
}

func (q *recoverySchedule) completed(store JobStore, snapshot JobRecord, result recoveryExecution, executionErr error) error {
	if result.terminal || result.repairFailed {
		delete(q.entries, snapshot.JobID)
		return nil
	}
	// Preserve only this session's history, not a later submission's generation.
	e := q.entries[snapshot.JobID]
	if e.specHash != result.specHash {
		e = retryEntry{}
	}
	snapshot.AttemptID = result.generation
	current, ok, err := store.Load(snapshot.JobID)
	if err == nil && (!ok || current.State != JobRecovering) {
		delete(q.entries, snapshot.JobID)
		return nil
	}
	if err == nil && (current.AttemptID != result.generation || recoverySpecHash(current) != result.specHash) {
		delete(q.entries, snapshot.JobID)
		q.recheck(current)
		return nil
	}
	if result.executed && executionErr != nil {
		e.failures = incrementRecoveryFailures(e.failures)
		e.generation, e.specHash = result.generation, result.specHash
		e.nextRetry = q.now().Add(recoveryBackoff(e.failures, q.sample))
		q.entries[snapshot.JobID] = e
	} else {
		q.recheck(snapshot)
	}
	return err
}

func (q *recoverySchedule) deadline() time.Time {
	// Even an overdue job cannot bypass a failed global scan's retry deadline.
	if !q.scan.nextRetry.IsZero() {
		return q.scan.nextRetry
	}
	var next time.Time
	for _, e := range q.entries {
		if next.IsZero() || e.nextRetry.Before(next) {
			next = e.nextRetry
		}
	}
	return next
}

// Only the recovery loop owns this timer and schedule. Tests provide a manual
// timer/clock; worker handlers never access either scheduling object.
type recoveryTimer interface {
	channel() <-chan time.Time
	reset(time.Duration)
	stop()
}

type realRecoveryTimer struct{ *time.Timer }

func (t realRecoveryTimer) channel() <-chan time.Time { return t.C }
func (t realRecoveryTimer) reset(d time.Duration)     { t.Reset(d) }
func (t realRecoveryTimer) stop()                     { t.Stop() }

func (s *CoordinatorServer) runRecoveryLoop(ctx context.Context) {
	timer := realRecoveryTimer{time.NewTimer(time.Hour)}
	timer.stop()
	s.runRecoveryScheduler(ctx, newRecoverySchedule(), timer)
}

func (s *CoordinatorServer) runRecoveryScheduler(ctx context.Context, q *recoverySchedule, timer recoveryTimer) {
	defer timer.stop()
	// Coalesce a pre-existing notification with initial discovery. Notifications
	// published during discovery remain queued for the following pass.
	select {
	case <-s.recoveryReady:
	default:
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if !q.now().Before(q.scan.nextRetry) {
			if err := s.runScheduledRecoveryPass(ctx, q); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("recovery pass failed: %v", err)
			}
		}
		if ctx.Err() != nil {
			return
		}
		timer.stop()
		var tick <-chan time.Time
		if deadline := q.deadline(); !deadline.IsZero() {
			delay := deadline.Sub(q.now())
			if delay < 0 {
				delay = 0
			}
			// Go 1.27 channel timers discard old ticks on Reset; no blocking drain.
			timer.reset(delay)
			tick = timer.channel()
		}
		select {
		case <-ctx.Done():
			return
		case <-s.recoveryReady:
		case <-tick:
		}
	}
}
