package main

import (
	"context"
	"errors"
	"flag"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

type clientExecutionConfig struct {
	JobTimeout time.Duration
	RPCTimeout time.Duration
}

func clientExecutionFlags(fs *flag.FlagSet) *clientExecutionConfig {
	c := &clientExecutionConfig{}
	fs.DurationVar(&c.JobTimeout, "job-timeout", 5*time.Second, "positive Worker task timeout in whole milliseconds")
	fs.DurationVar(&c.RPCTimeout, "rpc-timeout", 30*time.Second, "positive SubmitJob RPC timeout")
	return c
}

func (c clientExecutionConfig) validate() error {
	if c.JobTimeout < time.Millisecond || c.JobTimeout%time.Millisecond != 0 {
		return errors.New("-job-timeout must be positive and representable in whole milliseconds")
	}
	if c.RPCTimeout <= 0 {
		return errors.New("-rpc-timeout must be positive")
	}
	return nil
}

// The Worker timeout and parent RPC deadline remain independent of the
// Coordinator's configured attempt lease. time.Duration bounds conversion to
// milliseconds, and validation prevents lossy sub-millisecond rounding.
func submitDemo(ctx context.Context, client runtimepb.CoordinatorServiceClient, c clientExecutionConfig) (*runtimepb.SubmitJobResponse, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
	defer cancel()
	payload, err := buildPreimagePayload("secret-001", demoPreimageDigest)
	if err != nil {
		return nil, err
	}
	return client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: 3001, TaskType: "zk_preimage_prove", Payload: payload,
		TimeoutMs: c.JobTimeout.Milliseconds(),
	})
}
