package main

import (
	"testing"
)

func TestValidJobTransitions(t *testing.T) {
	tests := []struct {
		from JobStatus
		to   JobStatus
		want bool
	}{
		{JobPending, JobRunning, true},

		{JobRunning, JobSucceeded, true},
		{JobRunning, JobFailed, true},
		{JobRunning, JobCancelled, true},

		{JobSucceeded, JobRunning, false},
		{JobFailed, JobRunning, false},
		{JobCancelled, JobRunning, false},

		{JobPending, JobSucceeded, false},
	}

	for _, tt := range tests {
		got := validTransition(tt.from, tt.to)

		if got != tt.want {
			t.Fatalf(
				"transition %v -> %v: expected %v, got %v",
				tt.from,
				tt.to,
				tt.want,
				got,
			)
		}
	}
}
