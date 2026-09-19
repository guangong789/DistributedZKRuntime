package main

import (
	"context"
	"testing"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func TestExecuteJobEchoesAttemptID(t *testing.T) {
	for _, tc := range []struct {
		name, taskType, payload, wantStatus string
		cancel                              bool
	}{
		{"success", "hash", "hello", "Succeeded", false},
		{"task failure", "hash", "", "Failed", false},
		{"unknown task", "unknown", "", "failed", false},
		{"invalid sleep", "sleep", "invalid", "failed", false},
		{"canceled", "sleep", "1s", "Cancelled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			resp, err := (&WorkerService{}).ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{
				JobId: 500, AttemptId: 7, TaskType: tc.taskType, Payload: tc.payload, TimeoutMs: 1000,
			})
			if err != nil || resp == nil || resp.JobId != 500 || resp.AttemptId != 7 || resp.Status != tc.wantStatus {
				t.Fatalf("unexpected response=%v error=%v", resp, err)
			}
		})
	}
}
