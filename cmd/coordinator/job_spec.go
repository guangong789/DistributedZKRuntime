package main

import (
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// validateJobSpec returns a value snapshot. Raw RPC/legacy ZK JSON cannot reach
// persistence or dispatch; other task types retain their existing payloads.
func validateJobSpec(spec JobSpec) (JobSpec, error) {
	if spec.TaskType == "zk_preimage_prove" {
		payload, err := zk.ParsePreimagePublicPayload(spec.Payload)
		if err != nil {
			return JobSpec{}, status.Error(codes.InvalidArgument, err.Error())
		}
		spec.Payload = payload.CanonicalJSON
	}
	return spec, nil
}
