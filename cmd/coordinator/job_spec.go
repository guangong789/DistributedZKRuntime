package main

import runtimepb "github.com/guangong789/DistributedZKRuntime/proto"

// JobSpec is a value snapshot of a job's inputs. Execution treats it as immutable.
type JobSpec struct {
	JobID     int64
	TaskType  string
	Payload   string
	TimeoutMs int64
}

func jobSpecFromRequest(req *runtimepb.SubmitJobRequest) JobSpec {
	return JobSpec{
		JobID:     req.JobId,
		TaskType:  req.TaskType,
		Payload:   req.Payload,
		TimeoutMs: req.TimeoutMs,
	}
}

func jobSpecFromRecord(record JobRecord) JobSpec {
	return JobSpec{
		JobID:     record.JobID,
		TaskType:  record.TaskType,
		Payload:   record.Payload,
		TimeoutMs: record.TimeoutMs,
	}
}
