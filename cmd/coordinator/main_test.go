package main

import (
	"testing"
	"time"
)

func TestLeaseCreatedForAttempt(t *testing.T) {
	s := &CoordinatorServer{
		leases: make(map[int64]JobLease),
	}

	s.setLease(
		500,
		2,
		"worker-1",
		time.Second,
	)

	lease, ok := s.getLease(500)
	if !ok {
		t.Fatal("expected lease")
	}

	if lease.AttemptID != 2 {
		t.Fatalf(
			"expected attempt 2, got %d",
			lease.AttemptID,
		)
	}

	if lease.WorkerID != "worker-1" {
		t.Fatalf(
			"expected worker-1, got %s",
			lease.WorkerID,
		)
	}
}

func TestIsLeaseCurrent(t *testing.T) {
	now := time.Now()

	s := &CoordinatorServer{
		leases: map[int64]JobLease{
			500: {
				JobID:     500,
				AttemptID: 2,
				WorkerID:  "worker-1",
				ExpiresAt: now.Add(time.Second),
			},
		},
	}

	if !s.isLeaseCurrent(500, 2) {
		t.Fatal("expected current valid lease")
	}

	if s.isLeaseCurrent(500, 1) {
		t.Fatal("old attempt should not be current")
	}

	s.mu.Lock()
	lease := s.leases[500]
	lease.ExpiresAt = time.Now().Add(-time.Second)
	s.leases[500] = lease
	s.mu.Unlock()

	if s.isLeaseCurrent(500, 2) {
		t.Fatal("expired lease should not be current")
	}
}

func TestLeaseExpiry(t *testing.T) {
	now := time.Now()

	s := &CoordinatorServer{
		leases: map[int64]JobLease{
			500: {
				JobID:     500,
				AttemptID: 1,
				WorkerID:  "worker-1",
				ExpiresAt: now.Add(time.Second),
			},
		},
	}

	if s.leaseExpired(500, now) {
		t.Fatal("lease should not be expired yet")
	}

	if !s.leaseExpired(
		500,
		now.Add(2*time.Second),
	) {
		t.Fatal("lease should be expired")
	}
}