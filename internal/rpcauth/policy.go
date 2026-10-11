package rpcauth

import (
	"context"
	"errors"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Policy is an immutable exact-identity allowlist. Clients share one authorization
// domain; this policy does not establish tenant or JobID ownership.
type Policy struct {
	coordinator Principal
	allowed     map[Principal]struct{}
}

func NewPolicy(coordinator Principal, clients, workers []Principal) (*Policy, error) {
	if !coordinator.valid() || coordinator.Role() != Coordinator {
		return nil, errors.New("coordinator principal required")
	}
	p := &Policy{coordinator: coordinator, allowed: map[Principal]struct{}{coordinator: {}}}
	for role, principals := range map[Role][]Principal{Client: clients, Worker: workers} {
		for _, principal := range principals {
			if !principal.valid() || principal.Role() != role {
				return nil, errors.New("allowlist principal has incorrect role")
			}
			p.allowed[principal] = struct{}{}
		}
	}
	return p, nil
}

func (p *Policy) recognizes(principal Principal) bool {
	if p == nil || !principal.valid() {
		return false
	}
	_, ok := p.allowed[principal]
	return ok
}

// AuthorizeRPC expects a principal from AuthenticatePeer, not request data.
// Unknown methods, mismatched request types and unrecognized identities deny.
func (p *Policy) AuthorizeRPC(principal Principal, method string, request any) error {
	allowed := false
	if p.recognizes(principal) {
		switch method {
		case runtimepb.CoordinatorService_SubmitJob_FullMethodName:
			req, ok := request.(*runtimepb.SubmitJobRequest)
			allowed = ok && req != nil && principal.Role() == Client
		case runtimepb.CoordinatorService_RegisterWorker_FullMethodName:
			req, ok := request.(*runtimepb.RegisterWorkerRequest)
			allowed = ok && req != nil && principal.Role() == Worker && req.WorkerId == principal.WorkerID()
		case runtimepb.CoordinatorService_Heartbeat_FullMethodName:
			req, ok := request.(*runtimepb.HeartbeatRequest)
			allowed = ok && req != nil && principal.Role() == Worker && req.WorkerId == principal.WorkerID()
		case runtimepb.WorkerService_ExecuteJob_FullMethodName:
			req, ok := request.(*runtimepb.ExecuteJobRequest)
			allowed = ok && req != nil && principal == p.coordinator
		}
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "RPC not permitted for peer identity")
	}
	return nil
}

// UnaryInterceptor authenticates and authorizes before entering the handler.
// Transport constructors must install it explicitly.
func (p *Policy) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		principal, err := AuthenticatePeer(ctx)
		if err != nil {
			return nil, err
		}
		if err := p.AuthorizeRPC(principal, info.FullMethod, req); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// UnknownServiceHandler closes the unregistered-method path, for which gRPC
// does not invoke unary interceptors. No business handler is entered.
func (p *Policy) UnknownServiceHandler() grpc.StreamHandler {
	return func(_ any, stream grpc.ServerStream) error {
		if _, err := AuthenticatePeer(stream.Context()); err != nil {
			return err
		}
		return status.Error(codes.PermissionDenied, "RPC method not permitted")
	}
}
