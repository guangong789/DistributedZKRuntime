package main

import (
	"errors"
	"time"
)

var ErrExecutorNotRunning = errors.New("executor is not running")
var ErrInvalidJob = errors.New("invalid job")
var ErrInvalidJobTransition = errors.New("invalid job state transition")
var ErrDuplicateJobID = errors.New("duplicate job id")
var ErrJobNotFound = errors.New("job not found")

type JobStatus int

const (
	JobPending JobStatus = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobCancelled
)

func validTransition(from, to JobStatus) bool {
	switch from {
	case JobPending:
		return to == JobRunning || to == JobCancelled

	case JobRunning:
		return to == JobSucceeded ||
			to == JobFailed ||
			to == JobCancelled

	case JobSucceeded, JobFailed, JobCancelled:
		return false

	default:
		return false
	}
}

func (s JobStatus) String() string {
	switch s {
	case JobPending:
		return "Pending"
	case JobRunning:
		return "Running"
	case JobSucceeded:
		return "Succeeded"
	case JobFailed:
		return "Failed"
	case JobCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

type Job struct {
	ID      int
	Timeout time.Duration
	Task    Task
	Status  JobStatus
}

type Result struct {
	JobID  int
	Status JobStatus
	Output string
	Err    error
}

type ExecutorState int

const (
	ExecutorRunning ExecutorState = iota
	ExecutorShuttingDown
	ExecutorStopped
)
