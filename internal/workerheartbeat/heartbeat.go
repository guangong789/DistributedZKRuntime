package workerheartbeat

import (
	"context"
	"fmt"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

// CoordinatorClient is the coordinator RPC subset needed by heartbeat recovery.
type CoordinatorClient interface {
	Heartbeat(context.Context, *runtimepb.HeartbeatRequest, ...grpc.CallOption) (*runtimepb.HeartbeatResponse, error)
	RegisterWorker(context.Context, *runtimepb.RegisterWorkerRequest, ...grpc.CallOption) (*runtimepb.RegisterWorkerResponse, error)
}

// Send sends one heartbeat and re-registers when the coordinator has lost the worker.
func Send(
	ctx context.Context,
	client CoordinatorClient,
	workerID string,
	workerAddress string,
) error {
	resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: workerID})
	if err != nil {
		return err
	}
	if resp.Accepted {
		return nil
	}

	registration, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{
		WorkerId: workerID,
		Address:  workerAddress,
	})
	if err != nil {
		return err
	}
	if !registration.Accepted {
		return fmt.Errorf("registration rejected: %s", registration.Error)
	}
	return nil
}
