package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

type fakeCoordinatorClient struct {
	heartbeat func(context.Context, *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error)
	register  func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error)
}

func (c *fakeCoordinatorClient) Heartbeat(ctx context.Context, req *runtimepb.HeartbeatRequest, _ ...grpc.CallOption) (*runtimepb.HeartbeatResponse, error) {
	return c.heartbeat(ctx, req)
}

func (c *fakeCoordinatorClient) RegisterWorker(ctx context.Context, req *runtimepb.RegisterWorkerRequest, _ ...grpc.CallOption) (*runtimepb.RegisterWorkerResponse, error) {
	return c.register(ctx, req)
}

func TestSendHeartbeat(t *testing.T) {
	heartbeatErr := errors.New("heartbeat RPC failed")
	registrationErr := errors.New("registration RPC failed")
	for _, tc := range []struct {
		name                 string
		heartbeatAccepted    bool
		heartbeatErr         error
		registrationAccepted bool
		registrationErr      error
		wantRPCError         error
		wantRejected         bool
		wantRegister         bool
	}{
		{name: "heartbeat accepted", heartbeatAccepted: true},
		{name: "heartbeat rejected", registrationAccepted: true, wantRegister: true},
		{name: "heartbeat RPC error", heartbeatErr: heartbeatErr, wantRPCError: heartbeatErr},
		{name: "re-registration RPC error", registrationErr: registrationErr, wantRPCError: registrationErr, wantRegister: true},
		{name: "re-registration rejected", wantRejected: true, wantRegister: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			const workerID = "worker-7"
			const address = "worker.example:50051"
			var calls []string
			client := &fakeCoordinatorClient{
				heartbeat: func(gotCtx context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
					calls = append(calls, "Heartbeat")
					if gotCtx != ctx || req.WorkerId != workerID {
						t.Errorf("heartbeat context or worker ID changed: %v", req)
					}
					if tc.heartbeatErr != nil {
						return nil, tc.heartbeatErr
					}
					return &runtimepb.HeartbeatResponse{Accepted: tc.heartbeatAccepted}, nil
				},
				register: func(gotCtx context.Context, req *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
					calls = append(calls, "RegisterWorker")
					if gotCtx != ctx || req.WorkerId != workerID || req.Address != address {
						t.Errorf("registration context, worker ID, or advertised address changed: %v", req)
					}
					if tc.registrationErr != nil {
						return nil, tc.registrationErr
					}
					return &runtimepb.RegisterWorkerResponse{Accepted: tc.registrationAccepted, Error: "registration denied"}, nil
				},
			}
			err := workerheartbeat.Send(ctx, client, workerID, address)
			switch {
			case tc.wantRPCError != nil:
				if err != tc.wantRPCError {
					t.Fatalf("got error %v, want original error %v", err, tc.wantRPCError)
				}
			case tc.wantRejected:
				if err == nil || err.Error() != "registration rejected: registration denied" {
					t.Fatalf("expected registration rejection, got %v", err)
				}
			default:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			wantCalls := []string{"Heartbeat"}
			if tc.wantRegister {
				wantCalls = append(wantCalls, "RegisterWorker")
			}
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf("RPC calls = %v, want %v", calls, wantCalls)
			}
			if ctx.Err() != nil {
				t.Fatalf("helper canceled the caller context: %v", ctx.Err())
			}
		})
	}
}
