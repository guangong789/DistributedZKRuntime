package main

import (
	"time"
)

type JobAttempt struct {
	JobID     int64
	AttemptID int64
	WorkerID  string
}

type JobLease struct {
	JobID     int64
	AttemptID int64
	WorkerID  string
	ExpiresAt time.Time
}

// startAttempt allocates the ID and publishes its lease atomically.
func (s *CoordinatorServer) startAttempt(jobID int64, workerID string, duration time.Duration) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.jobAttempts == nil {
		s.jobAttempts = make(map[int64]int64)
	}
	next := s.jobAttempts[jobID] + 1
	s.jobAttempts[jobID] = next
	s.setLeaseLocked(jobID, next, workerID, duration)
	return next
}

func (s *CoordinatorServer) setLease(
	jobID int64,
	attemptID int64,
	workerID string,
	duration time.Duration,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setLeaseLocked(jobID, attemptID, workerID, duration)
}

// setLeaseLocked requires s.mu to be held.
func (s *CoordinatorServer) setLeaseLocked(jobID, attemptID int64, workerID string, duration time.Duration) {
	if s.leases == nil {
		s.leases = make(map[int64]JobLease)
	}
	s.leases[jobID] = JobLease{
		JobID:     jobID,
		AttemptID: attemptID,
		WorkerID:  workerID,
		ExpiresAt: time.Now().Add(duration),
	}
}

func (s *CoordinatorServer) getLease(
	jobID int64,
) (JobLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[jobID]
	return lease, ok
}

func (s *CoordinatorServer) leaseExpired(
	jobID int64,
	now time.Time,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[jobID]
	if !ok {
		return false
	}

	return !now.Before(lease.ExpiresAt)
}

func (s *CoordinatorServer) isLeaseCurrent(
	jobID int64,
	attemptID int64,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[jobID]
	if !ok {
		return false
	}

	if lease.AttemptID != attemptID {
		return false
	}

	return time.Now().Before(lease.ExpiresAt)
}

func (s *CoordinatorServer) claimJob(jobID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeJobs == nil {
		s.activeJobs = make(map[int64]struct{})
	}

	if _, exists := s.activeJobs[jobID]; exists {
		return false
	}

	s.activeJobs[jobID] = struct{}{}
	return true
}

func (s *CoordinatorServer) releaseJob(jobID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.activeJobs, jobID)
}
