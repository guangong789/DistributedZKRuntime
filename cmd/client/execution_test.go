package main

import (
	"context"
	"flag"
	"io"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

type demoSubmitClient struct {
	runtimepb.CoordinatorServiceClient
	submit func(context.Context, *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error)
}

func (c demoSubmitClient) SubmitJob(ctx context.Context, req *runtimepb.SubmitJobRequest, _ ...grpc.CallOption) (*runtimepb.SubmitJobResponse, error) {
	return c.submit(ctx, req)
}

func TestClientExecutionTimeBudgets(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		job, rpc time.Duration
	}{
		{"defaults", nil, 5 * time.Second, 30 * time.Second},
		{"slow proof", []string{"-job-timeout", "40s", "-rpc-timeout", "90s"}, 40 * time.Second, 90 * time.Second},
		{"independent shorter RPC", []string{"-job-timeout", "40s", "-rpc-timeout", "2s"}, 40 * time.Second, 2 * time.Second},
		{"maximum whole milliseconds", []string{"-job-timeout", "9223372036854ms"}, 9223372036854 * time.Millisecond, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("client", flag.ContinueOnError)
			config := clientExecutionFlags(fs)
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if err := config.validate(); err != nil || config.JobTimeout != tc.job || config.RPCTimeout != tc.rpc {
				t.Fatalf("config=%+v error=%v", config, err)
			}
			calls := 0
			before := time.Now()
			client := demoSubmitClient{submit: func(ctx context.Context, req *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || deadline.Before(before.Add(tc.rpc)) || deadline.After(time.Now().Add(tc.rpc)) {
					t.Fatal("configured RPC duration did not reach SubmitJob context")
				}
				payload, _ := buildPreimagePayload("secret-001", demoPreimageDigest)
				if req.JobId != 3001 || req.TaskType != "zk_preimage_prove" || req.Payload != payload || req.TimeoutMs != tc.job.Milliseconds() {
					t.Fatal("configured task timeout or public demo request changed")
				}
				return &runtimepb.SubmitJobResponse{JobId: req.JobId, Status: "Succeeded"}, nil
			}}
			if response, err := submitDemo(context.Background(), client, *config); err != nil || response.GetStatus() != "Succeeded" || calls != 1 {
				t.Fatalf("demo submit response=%v error=%v calls=%d", response, err, calls)
			}
		})
	}
}

func TestClientExecutionRejectsInvalidDurations(t *testing.T) {
	for _, args := range [][]string{
		{"-job-timeout", "0"}, {"-job-timeout", "-1s"}, {"-job-timeout", "500us"}, {"-job-timeout", "1s1ns"},
		{"-job-timeout", "9223372036854775807ns"}, {"-job-timeout", "9223372036855ms"}, {"-job-timeout", "invalid"},
		{"-rpc-timeout", "0"}, {"-rpc-timeout", "-1s"}, {"-rpc-timeout", "9223372036854775808ns"}, {"-rpc-timeout", "invalid"},
	} {
		t.Run(args[0]+"/"+args[1], func(t *testing.T) {
			fs := flag.NewFlagSet("client", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			config := clientExecutionFlags(fs)
			if err := fs.Parse(args); err == nil && config.validate() == nil {
				t.Fatal("invalid/unrepresentable duration accepted")
			}
		})
	}
	called := false
	client := demoSubmitClient{submit: func(context.Context, *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
		called = true
		return nil, nil
	}}
	if _, err := submitDemo(context.Background(), client, clientExecutionConfig{}); err == nil || called {
		t.Fatal("invalid configuration reached the RPC")
	}
}

func TestDemoSubmitPreservesEarlierParentDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := parent.Deadline()
	client := demoSubmitClient{submit: func(ctx context.Context, _ *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
		if got, _ := ctx.Deadline(); got != want {
			t.Fatal("RPC configuration extended parent's deadline")
		}
		return &runtimepb.SubmitJobResponse{}, nil
	}}
	if _, err := submitDemo(parent, client, clientExecutionConfig{JobTimeout: 40 * time.Second, RPCTimeout: 90 * time.Second}); err != nil {
		t.Fatal(err)
	}
}
