package rpcauth

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type authenticatedService struct {
	runtimepb.UnimplementedCoordinatorServiceServer
	runtimepb.UnimplementedWorkerServiceServer
	calls atomic.Int32
	seen  chan Principal
}

func (s *authenticatedService) entered(ctx context.Context) error {
	p, err := AuthenticatePeer(ctx)
	if err != nil {
		return err
	}
	s.calls.Add(1)
	s.seen <- p
	return nil
}
func (s *authenticatedService) SubmitJob(ctx context.Context, _ *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	if err := s.entered(ctx); err != nil {
		return nil, err
	}
	return &runtimepb.SubmitJobResponse{Status: "Succeeded"}, nil
}
func (s *authenticatedService) RegisterWorker(ctx context.Context, _ *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
	if err := s.entered(ctx); err != nil {
		return nil, err
	}
	return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
}
func (s *authenticatedService) Heartbeat(ctx context.Context, _ *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
	if err := s.entered(ctx); err != nil {
		return nil, err
	}
	return &runtimepb.HeartbeatResponse{Accepted: true}, nil
}
func (s *authenticatedService) ExecuteJob(ctx context.Context, _ *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	if err := s.entered(ctx); err != nil {
		return nil, err
	}
	return &runtimepb.ExecuteJobResponse{Status: "Succeeded"}, nil
}

func startAuthenticatedServer(t *testing.T, config *tls.Config, policy *Policy) (string, *authenticatedService) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)), grpc.UnaryInterceptor(policy.UnaryInterceptor()))
	service := &authenticatedService{seen: make(chan Principal, 1)}
	runtimepb.RegisterCoordinatorServiceServer(server, service)
	runtimepb.RegisterWorkerServiceServer(server, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String(), service
}

func TestLocalhostGRPCAuthenticationAndAuthorization(t *testing.T) {
	ca := newTestCA(t)
	policy := testPolicy(t)
	serverMaterial := loadTestMaterial(t, ca.issue(t, coordURI, nil), ca, coordURI)
	sc, err := NewServerTLS(serverMaterial, policy)
	if err != nil {
		t.Fatal(err)
	}
	address, service := startAuthenticatedServer(t, sc, policy)
	clients := map[string]*grpc.ClientConn{}
	for _, uri := range []string{clientURI, workerURI, coordURI} {
		m := loadTestMaterial(t, ca.issue(t, uri, nil), ca, uri)
		cfg, err := NewClientTLS(m, "127.0.0.1", principal(t, coordURI))
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		clients[uri] = conn
	}
	for _, tc := range []struct {
		name, caller, method string
		request, response    any
		allowed              bool
	}{
		{"client submission", clientURI, runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{}, &runtimepb.SubmitJobResponse{}, true},
		{"client cannot register with forged metadata", clientURI, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a"}, &runtimepb.RegisterWorkerResponse{}, false},
		{"worker registration", workerURI, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "localhost:1234"}, &runtimepb.RegisterWorkerResponse{}, true},
		{"worker cannot register another ID", workerURI, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b"}, &runtimepb.RegisterWorkerResponse{}, false},
		{"worker heartbeat", workerURI, runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}, &runtimepb.HeartbeatResponse{}, true},
		{"worker cannot heartbeat another ID", workerURI, runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-b"}, &runtimepb.HeartbeatResponse{}, false},
		{"worker cannot submit", workerURI, runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{}, &runtimepb.SubmitJobResponse{}, false},
		{"coordinator execution", coordURI, runtimepb.WorkerService_ExecuteJob_FullMethodName, &runtimepb.ExecuteJobRequest{}, &runtimepb.ExecuteJobResponse{}, true},
		{"client cannot execute", clientURI, runtimepb.WorkerService_ExecuteJob_FullMethodName, &runtimepb.ExecuteJobRequest{}, &runtimepb.ExecuteJobResponse{}, false},
		{"worker cannot execute", workerURI, runtimepb.WorkerService_ExecuteJob_FullMethodName, &runtimepb.ExecuteJobRequest{}, &runtimepb.ExecuteJobResponse{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("role", "coordinator", "identity", coordURI, "worker-id", "worker-b"))
			before := service.calls.Load()
			err := clients[tc.caller].Invoke(ctx, tc.method, tc.request, tc.response)
			if tc.allowed {
				if err != nil {
					t.Fatal(err)
				}
				if service.calls.Load() != before+1 {
					t.Fatal("authorized request did not reach handler exactly once")
				}
				select {
				case p := <-service.seen:
					if p != principal(t, tc.caller) {
						t.Fatal("metadata forged identity")
					}
				case <-ctx.Done():
					t.Fatal("handler did not report principal")
				}
			} else {
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("error = %v", err)
				}
				if service.calls.Load() != before {
					t.Fatal("unauthorized request caused handler side effects")
				}
			}
		})
	}
}

func TestGRPCRejectsWrongWorkerEndpoint(t *testing.T) {
	ca := newTestCA(t)
	policy := testPolicy(t)
	worker := loadTestMaterial(t, ca.issue(t, workerURI, nil), ca, workerURI)
	sc, err := NewServerTLS(worker, policy)
	if err != nil {
		t.Fatal(err)
	}
	address, service := startAuthenticatedServer(t, sc, policy)
	coordinator := loadTestMaterial(t, ca.issue(t, coordURI, nil), ca, coordURI)
	for _, expected := range []string{workerBURI, workerURI} {
		t.Run(expected, func(t *testing.T) {
			cfg, err := NewClientTLS(coordinator, "127.0.0.1", principal(t, expected))
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resp, err := runtimepb.NewWorkerServiceClient(conn).ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{})
			if expected == workerBURI {
				if err == nil || resp != nil {
					t.Fatal("wrong endpoint returned an accepted RPC response")
				}
				if service.calls.Load() != 0 {
					t.Fatal("wrong endpoint reached handler")
				}
			} else {
				if err != nil || resp.Status != "Succeeded" || service.calls.Load() != 1 {
					t.Fatalf("correct endpoint failed: %v", err)
				}
				select {
				case p := <-service.seen:
					if p != principal(t, coordURI) {
						t.Fatal("wrong caller")
					}
				case <-ctx.Done():
					t.Fatal("no handler identity")
				}
			}
		})
	}
}

func TestUnaryInterceptorRejectsUnauthenticatedBeforeHandler(t *testing.T) {
	called := false
	_, err := testPolicy(t).UnaryInterceptor()(metadata.NewIncomingContext(context.Background(), metadata.Pairs("identity", clientURI)), &runtimepb.SubmitJobRequest{}, &grpc.UnaryServerInfo{FullMethod: runtimepb.CoordinatorService_SubmitJob_FullMethodName}, func(context.Context, any) (any, error) { called = true; return nil, nil })
	if status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("authentication bypass: called=%v error=%v", called, err)
	}
}
