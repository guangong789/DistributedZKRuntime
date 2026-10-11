package rpcauth

import (
	"errors"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// NewWorkerServer is the production Worker transport constructor. Only the
// configured Coordinator can ExecuteJob; authorization precedes the handler.
func NewWorkerServer(local *Material, coordinator Principal, service runtimepb.WorkerServiceServer) (*grpc.Server, error) {
	if !local.valid() || local.principal.Role() != Worker || service == nil {
		return nil, errors.New("Worker server credentials and service required")
	}
	policy, err := NewPolicy(coordinator, nil, []Principal{local.principal})
	if err != nil {
		return nil, err
	}
	config, err := NewServerTLS(local, policy)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)),
		grpc.UnaryInterceptor(policy.UnaryInterceptor()), grpc.UnknownServiceHandler(policy.UnknownServiceHandler()))
	runtimepb.RegisterWorkerServiceServer(server, service)
	return server, nil
}
