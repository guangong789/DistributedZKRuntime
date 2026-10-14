package main

import (
	"context"
	"crypto/x509"
	"flag"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type workerTLSCoordinator struct {
	runtimepb.UnimplementedCoordinatorServiceServer
	mu             sync.Mutex
	registered     bool
	registrations  int
	accepted       []bool
	ids, addresses []string
	principals     []rpcauth.Principal
}

func (s *workerTLSCoordinator) RegisterWorker(ctx context.Context, r *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
	p, err := rpcauth.AuthenticatePeer(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registered = true
	s.registrations++
	s.ids = append(s.ids, r.WorkerId)
	s.addresses = append(s.addresses, r.Address)
	s.principals = append(s.principals, p)
	return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
}
func (s *workerTLSCoordinator) Heartbeat(ctx context.Context, r *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
	p, err := rpcauth.AuthenticatePeer(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepted = append(s.accepted, s.registered)
	s.ids = append(s.ids, r.WorkerId)
	s.principals = append(s.principals, p)
	return &runtimepb.HeartbeatResponse{Accepted: s.registered}, nil
}
func startWorkerTLSCoordinator(t *testing.T, ca *testpki.CA, identity string, change ...func(*x509.Certificate)) (string, *workerTLSCoordinator) {
	t.Helper()
	var edit func(*x509.Certificate)
	if len(change) != 0 {
		edit = change[0]
	}
	files := ca.Issue(t, identity, edit)
	serverPrincipal, err := rpcauth.ParsePrincipal(identity)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	worker, _ := rpcauth.ParsePrincipal(testpki.Worker)
	policy, err := rpcauth.NewPolicy(coordinator, nil, []rpcauth.Principal{worker})
	if err != nil {
		t.Fatal(err)
	}
	material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, serverPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := rpcauth.NewServerTLS(material, policy)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.UnaryInterceptor(policy.UnaryInterceptor()))
	service := &workerTLSCoordinator{}
	runtimepb.RegisterCoordinatorServiceServer(server, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String(), service
}

func TestWorkerMTLSEstablishedConnectionRejectsExpiredCredentials(t *testing.T) {
	for _, expiredPeer := range []string{"Coordinator", "Worker"} {
		t.Run(expiredPeer, func(t *testing.T) {
			ca := testpki.New(t)
			// Wait for the certificate's actual second-resolution expiry, rather
			// than an arbitrary sleep or changing already-verified certificates.
			expires := time.Now().Add(2 * time.Second).Truncate(time.Second)
			shortLived := func(c *x509.Certificate) { c.NotAfter = expires }
			var serverEdit, workerEdit func(*x509.Certificate)
			if expiredPeer == "Coordinator" {
				serverEdit = shortLived
			} else {
				workerEdit = shortLived
			}
			address, service := startWorkerTLSCoordinator(t, ca, testpki.Coordinator, serverEdit)
			files := ca.Issue(t, testpki.Worker, workerEdit)
			transport, err := workerCoordinatorCredentials("worker-a", address, rpcauth.CoordinatorClientConfig{
				CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator,
			})
			if err != nil {
				t.Fatal(err)
			}
			conn, err := newCoordinatorConnection(address, transport)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := runtimepb.NewCoordinatorServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "localhost:50051"}); err != nil {
				t.Fatalf("registration before expiry: %v", err)
			}
			if resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}); err != nil || !resp.GetAccepted() {
				t.Fatalf("heartbeat before expiry: %v", err)
			}
			timer := time.NewTimer(time.Until(expires.Add(time.Millisecond)))
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
				case <-ctx.Done():
					t.Fatal("timed out awaiting certificate expiry")
				}
				// TLS validity uses wall time; timers use monotonic time. Recheck
				// the wall-clock boundary in case the host clock was adjusted.
				if time.Now().After(expires) {
					break
				}
				timer.Reset(time.Until(expires.Add(time.Millisecond)))
			}
			if resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}); status.Code(err) != codes.Unauthenticated || resp != nil {
				t.Fatalf("expired %s retained authority: response=%v error=%v", expiredPeer, resp, err)
			}
			if resp, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "localhost:50051"}); status.Code(err) != codes.Unauthenticated || resp != nil {
				t.Fatalf("expired %s re-registered: response=%v error=%v", expiredPeer, resp, err)
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if service.registrations != 1 || !reflect.DeepEqual(service.accepted, []bool{true}) {
				t.Fatal("expired credentials reached registry/heartbeat handlers")
			}
		})
	}
}

func TestWorkerMTLSRegistrationAndHeartbeatReuse(t *testing.T) {
	ca := testpki.New(t)
	address, service := startWorkerTLSCoordinator(t, ca, testpki.Coordinator)
	files := ca.Issue(t, testpki.Worker, nil)
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	config := workerSecurityFlags(fs)
	if err := fs.Parse([]string{"-tls-cert", files.Cert, "-tls-key", files.Key, "-tls-ca", files.CA}); err != nil {
		t.Fatal(err)
	}
	transport, err := workerCoordinatorCredentials("worker-a", address, *config)
	if err != nil {
		t.Fatal(err)
	}
	const advertised = "worker.example:50051"
	// Exercise the actual initial-registration path and its three-second timeout.
	if err := registerWithCoordinator(context.Background(), "worker-a", advertised, address, transport); err != nil {
		t.Fatal(err)
	}
	// Exercise the actual connection constructor used by runHeartbeat; retain this
	// connection across accepted/rejected heartbeats and the recovery registration.
	conn, err := newCoordinatorConnection(address, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := runtimepb.NewCoordinatorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("identity", "spiffe://distributedzk.local/worker/worker-b", "worker-id", "worker-b"))
	if err := workerheartbeat.Send(ctx, client, "worker-a", advertised); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	service.registered = false
	service.mu.Unlock()
	if err := workerheartbeat.Send(ctx, client, "worker-a", advertised); err != nil {
		t.Fatal(err)
	}
	if err := workerheartbeat.Send(ctx, client, "worker-a", advertised); err != nil {
		t.Fatal(err)
	}
	// Certificate Worker A cannot use even the startup helper to impersonate B.
	if err := registerWithCoordinator(context.Background(), "worker-b", advertised, address, transport); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("WorkerID impersonation: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.registrations != 2 || !reflect.DeepEqual(service.accepted, []bool{true, false, true}) || !reflect.DeepEqual(service.addresses, []string{advertised, advertised}) {
		t.Fatal("heartbeat recovery did not reuse authenticated registration behavior")
	}
	for i, p := range service.principals {
		if p.Role() != rpcauth.Worker || p.WorkerID() != "worker-a" || service.ids[i] != "worker-a" {
			t.Fatal("request or metadata changed authenticated Worker identity")
		}
	}
}

func TestWorkerMTLSRejectsWrongCoordinatorEndpoint(t *testing.T) {
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Worker, nil)
	for _, tc := range []struct{ name, identity, serverName string }{
		{"wrong URI", testpki.Worker, ""}, {"wrong DNS SAN", testpki.Coordinator, "wrong.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, service := startWorkerTLSCoordinator(t, ca, tc.identity)
			config := rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator, ServerName: tc.serverName}
			transport, err := workerCoordinatorCredentials("worker-a", address, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := registerWithCoordinator(context.Background(), "worker-a", "worker.example:50051", address, transport); err == nil {
				t.Fatal("wrong Coordinator endpoint accepted")
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if service.registrations != 0 {
				t.Fatal("wrong Coordinator endpoint reached registration handler")
			}
		})
	}
}

func TestWorkerMTLSConfigurationFailsClosed(t *testing.T) {
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Worker, nil)
	valid := rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}
	for _, tc := range []struct {
		name, id string
		change   func(*rpcauth.CoordinatorClientConfig)
	}{
		{name: "missing cert", id: "worker-a", change: func(c *rpcauth.CoordinatorClientConfig) { c.CertFile = "" }},
		{name: "missing key", id: "worker-a", change: func(c *rpcauth.CoordinatorClientConfig) { c.KeyFile = "" }},
		{name: "missing CA", id: "worker-a", change: func(c *rpcauth.CoordinatorClientConfig) { c.CAFile = "" }},
		{name: "wrong key", id: "worker-a", change: func(c *rpcauth.CoordinatorClientConfig) { c.KeyFile = ca.Issue(t, testpki.Worker, nil).Key }},
		{name: "wrong WorkerID", id: "worker-b"}, {name: "missing WorkerID", id: ""},
		{name: "invalid Coordinator identity", id: "worker-a", change: func(c *rpcauth.CoordinatorClientConfig) { c.ServerIdentity = testpki.Client }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			if tc.change != nil {
				tc.change(&c)
			}
			transport, err := workerCoordinatorCredentials(tc.id, "localhost:50050", c)
			if err == nil || transport != nil {
				t.Fatal("invalid startup credentials or mismatched WorkerID accepted")
			}
		})
	}
	if conn, err := newCoordinatorConnection("localhost:50050", nil); err == nil || conn != nil {
		t.Fatal("nil credentials caused plaintext fallback")
	}
}
