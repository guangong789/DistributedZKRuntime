package main

import (
	"context"
	"flag"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type clientTLSCoordinator struct {
	runtimepb.UnimplementedCoordinatorServiceServer
	calls    atomic.Int32
	identity chan rpcauth.Principal
	requests chan *runtimepb.SubmitJobRequest
}

func (s *clientTLSCoordinator) SubmitJob(ctx context.Context, r *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	p, err := rpcauth.AuthenticatePeer(ctx)
	if err != nil {
		return nil, err
	}
	s.calls.Add(1)
	s.identity <- p
	if s.requests != nil {
		s.requests <- r
	}
	return &runtimepb.SubmitJobResponse{JobId: r.JobId, AttemptId: 1, Status: "Succeeded"}, nil
}

func TestClientMTLSProductionDial(t *testing.T) {
	ca := testpki.New(t)
	for _, tc := range []struct {
		name, serverIdentity, serverName string
		success                          bool
	}{
		{"trusted Coordinator IP SAN", testpki.Coordinator, "", true},
		{"explicit DNS SAN", testpki.Coordinator, "localhost", true},
		{"wrong Coordinator URI", testpki.Worker, "", false},
		{"wrong DNS SAN", testpki.Coordinator, "wrong.example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverFiles := ca.Issue(t, tc.serverIdentity, nil)
			serverIdentity, err := rpcauth.ParsePrincipal(tc.serverIdentity)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
			clientIdentity, _ := rpcauth.ParsePrincipal(testpki.Client)
			workerIdentity, _ := rpcauth.ParsePrincipal(testpki.Worker)
			policy, err := rpcauth.NewPolicy(coordinator, []rpcauth.Principal{clientIdentity}, []rpcauth.Principal{workerIdentity})
			if err != nil {
				t.Fatal(err)
			}
			material, err := rpcauth.LoadMaterial(serverFiles.Cert, serverFiles.Key, serverFiles.CA, serverIdentity)
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
			service := &clientTLSCoordinator{identity: make(chan rpcauth.Principal, 1), requests: make(chan *runtimepb.SubmitJobRequest, 1)}
			runtimepb.RegisterCoordinatorServiceServer(server, service)
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); <-done })
			clientFiles := ca.Issue(t, testpki.Client, nil)
			fs := flag.NewFlagSet("client", flag.ContinueOnError)
			config := clientSecurityFlags(fs)
			if err := fs.Parse([]string{"-tls-cert", clientFiles.Cert, "-tls-key", clientFiles.Key, "-tls-ca", clientFiles.CA, "-identity", testpki.Client, "-coordinator-server-name", tc.serverName}); err != nil {
				t.Fatal(err)
			}
			transport, err := config.credentials(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(transport))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			payload, err := buildPreimagePayload("secret-001", demoPreimageDigest)
			if err != nil {
				t.Fatal(err)
			}
			response, err := runtimepb.NewCoordinatorServiceClient(conn).SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 100, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 5000})
			if tc.success {
				if err != nil || response.GetStatus() != "Succeeded" || response.GetJobId() != 100 || service.calls.Load() != 1 {
					t.Fatalf("client production TLS dial: %v", err)
				}
				select {
				case p := <-service.identity:
					if p != clientIdentity {
						t.Fatal("wrong authenticated Client identity")
					}
				case <-ctx.Done():
					t.Fatal("no authenticated request")
				}
				<-service.requests // Original request, whose assertions remain above.
				response, err = submitDemo(ctx, runtimepb.NewCoordinatorServiceClient(conn), clientExecutionConfig{JobTimeout: 40 * time.Second, RPCTimeout: 90 * time.Second})
				if err != nil || response.GetJobId() != 3001 || service.calls.Load() != 2 {
					t.Fatalf("configured demo over mTLS: %v", err)
				}
				select {
				case request := <-service.requests:
					if request.TimeoutMs != 40000 || request.Payload != payload || request.TaskType != "zk_preimage_prove" {
						t.Fatal("configured task timeout did not reach the real mTLS RPC")
					}
				case <-ctx.Done():
					t.Fatal("configured demo request was not received")
				}
			} else if err == nil || response != nil || service.calls.Load() != 0 {
				t.Fatal("wrong Coordinator URI or DNS/IP SAN accepted")
			}
		})
	}
}

func TestClientMTLSConfigurationFailsClosed(t *testing.T) {
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Client, nil)
	valid := clientSecurityConfig{CoordinatorClientConfig: rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}, Identity: testpki.Client}
	for _, tc := range []struct {
		name   string
		change func(*clientSecurityConfig)
	}{
		{"missing cert", func(c *clientSecurityConfig) { c.CertFile = "" }}, {"missing key", func(c *clientSecurityConfig) { c.KeyFile = "" }}, {"missing CA", func(c *clientSecurityConfig) { c.CAFile = "" }},
		{"missing file", func(c *clientSecurityConfig) { c.CertFile = "does-not-exist" }}, {"key mismatch", func(c *clientSecurityConfig) { c.KeyFile = ca.Issue(t, testpki.Client, nil).Key }},
		{"wrong local role", func(c *clientSecurityConfig) { c.Identity = testpki.Worker }}, {"wrong local ID", func(c *clientSecurityConfig) { c.Identity = "spiffe://distributedzk.local/client/other" }},
		{"wrong expected server role", func(c *clientSecurityConfig) { c.ServerIdentity = testpki.Worker }}, {"bad server identity", func(c *clientSecurityConfig) { c.ServerIdentity = "*" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.change(&c)
			transport, err := c.credentials("localhost:50050")
			if err == nil || transport != nil {
				t.Fatal("configuration error produced usable credentials or plaintext fallback")
			}
		})
	}
	if transport, err := valid.credentials(":50050"); err == nil || transport != nil {
		t.Fatal("missing dial hostname accepted")
	}
}
