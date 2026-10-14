package main

import (
	"context"
	"crypto/x509"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func registerMTLSExecutionWorker(t *testing.T, f *mtlsCoordinator, identity, id, address string) runtimepb.CoordinatorServiceClient {
	t.Helper()
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, identity))
	response, err := client.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: id, Address: address})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("authenticated Worker registration: %v", err)
	}
	return client
}

func TestMTLSDispatchIdentityMismatchRetriesVerifiedWorker(t *testing.T) {
	f := newMTLSCoordinator(t)
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	var calls atomic.Int32
	proving := &admissionProvingMetrics{}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	execute := limitedPreimageWorker(fixture, limiter, proving, &calls)
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		p, err := rpcauth.AuthenticatePeer(ctx)
		if err != nil || p.Role() != rpcauth.Coordinator {
			t.Error("Worker did not authenticate Coordinator")
		}
		return execute(ctx, r)
	})
	// Both advertised hosts match their registering certificates, but the first
	// selected logical Worker B points to the TLS server owned by Worker A.
	registerMTLSExecutionWorker(t, f, workerBIdentity, "worker-b", address)
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	response, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 11101, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: preimageTestTimeoutMs})
	if err != nil || response.GetStatus() != "Succeeded" || response.GetAttemptId() != 2 {
		t.Fatalf("expected retry to verified Worker A: %v", err)
	}
	assertWorkerFailureSaves(t, f.store, []JobRecord{
		preimageRecord(11101, 1, "worker-b", JobRunning, fixture.payload), preimageRecord(11101, 2, "worker-a", JobRunning, fixture.payload), preimageRecord(11101, 2, "worker-a", JobSucceeded, fixture.payload),
	})
	if calls.Load() != 1 || proving.samples.Load() != 1 || f.observer.count("verification", "") != 1 || f.observer.count("finished", string(metrics.AttemptRPCError)) != 1 || f.observer.count("finished", string(metrics.AttemptAccepted)) != 1 || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 || f.observer.count("terminal", string(metrics.TerminalFailed)) != 0 {
		t.Fatal("identity mismatch reached execution, became a business failure, or fabricated success/proving metrics")
	}
	assertPublicRecoveryPayload(t, fixture.payload)
	assertRecoveryOwnershipReleased(t, f.s, 11101)
}

func TestMTLSDispatchRejectsBadEndpointsAsInfrastructureAttempts(t *testing.T) {
	for _, scenario := range []string{"wrong Worker URI", "wrong hostname", "untrusted Worker", "plaintext Worker", "missing dispatch credentials"} {
		t.Run(scenario, func(t *testing.T) {
			f := newMTLSCoordinator(t)
			var calls atomic.Int32
			execute := func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(r), nil
			}
			var address string
			switch scenario {
			case "wrong Worker URI":
				address = startMTLSBackend(t, f, workerBIdentity, execute)
			case "wrong hostname", "untrusted Worker":
				issuer := f.ca
				var change func(*x509.Certificate)
				if scenario == "untrusted Worker" {
					issuer = testpki.New(t)
				} else {
					change = func(c *x509.Certificate) { c.DNSNames = []string{"other.example"}; c.IPAddresses = nil }
				}
				files := issuer.Issue(t, testpki.Worker, change)
				p, _ := rpcauth.ParsePrincipal(testpki.Worker)
				material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, p)
				if err != nil {
					t.Fatal(err)
				}
				coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
				server, err := rpcauth.NewWorkerServer(material, coordinator, &testWorker{execute: execute})
				if err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address = listener.Addr().String()
				done := make(chan struct{})
				go func() { defer close(done); _ = server.Serve(listener) }()
				t.Cleanup(func() { server.Stop(); <-done })
			case "plaintext Worker":
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address = listener.Addr().String()
				server := grpc.NewServer()
				runtimepb.RegisterWorkerServiceServer(server, &testWorker{execute: execute})
				done := make(chan struct{})
				go func() { defer close(done); _ = server.Serve(listener) }()
				t.Cleanup(func() { server.Stop(); <-done })
			case "missing dispatch credentials":
				address = startMTLSBackend(t, f, testpki.Worker, execute)
				f.s.workerCredentials = nil
			}
			// Model an internally supplied/old registry snapshot to test dispatch's own
			// mandatory checks, independent of the registration SAN gate.
			f.s.workers = map[string]WorkerInfo{"worker-a": {ID: "worker-a", Address: address, Status: WorkerAlive, LastSeen: time.Now()}}
			f.s.workerOrder = []string{"worker-a"}
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			response, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 11102, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
			if response != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("transport failure classified as task result: %v", err)
			}
			writes := f.store.savedRecords()
			if len(writes) != 3 || writes[0].State != JobRunning || writes[0].AttemptID != 1 || writes[1].State != JobRunning || writes[1].AttemptID != 2 || writes[2].State != JobFailed || writes[2].AttemptID != 2 || calls.Load() != 0 {
				t.Fatal("failed authentication executed a handler or failed to finalize the session")
			}
			if f.observer.count("started", "") != 2 || f.observer.count("finished", string(metrics.AttemptRPCError)) != 2 || f.observer.count("terminal", "") != 1 || f.observer.count("terminal", string(metrics.TerminalFailed)) != 1 {
				t.Fatal("transport failures did not retain legitimate attempt accounting")
			}
			assertRecoveryOwnershipReleased(t, f.s, 11102)
		})
	}
}

func TestMTLSRegistrationEndpointValidationHasNoSideEffects(t *testing.T) {
	f := newMTLSCoordinator(t)
	address := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	client := registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"dns:///localhost:" + port, "unix:///tmp/socket", ":" + port, "localhost:", "localhost:0", "localhost:65536", "localhost:080", "localhost:" + port + "?x=1", "localhost%2e:" + port} {
		before := f.snapshot()
		r, err := client.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: endpoint})
		if r != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("malformed endpoint accepted: %v", err)
		}
		f.assertUnchanged(t, before, 11103)
	}
	before := f.snapshot()
	if r, err := client.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "not-in-certificate.example:" + port}); r != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("advertised host not bound to verified certificate: %v", err)
	}
	f.assertUnchanged(t, before, 11103)
	// DNS and IPv4 advertisement use certificate SANs, not source-IP equality.
	r, err := client.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "localhost:" + port})
	_, ok, order := coordinatorWorkerSnapshot(f.s, "worker-a")
	if err != nil || !r.GetAccepted() || !ok || !reflect.DeepEqual(order, []string{"worker-a"}) {
		t.Fatal("valid DNS advertisement rejected or duplicated scheduling order")
	}
	// The registration source is IPv4, while the advertised IPv6 host is covered
	// by the authenticated certificate. Source-IP equality is not required.
	files := f.ca.Issue(t, workerBIdentity, func(c *x509.Certificate) { c.IPAddresses = []net.IP{net.ParseIP("::1")} })
	local, _ := rpcauth.ParsePrincipal(workerBIdentity)
	transport, err := (rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}).Credentials(f.address, local)
	if err != nil {
		t.Fatal(err)
	}
	ipv6 := runtimepb.NewCoordinatorServiceClient(f.connect(t, transport))
	advertised := "[::1]:" + port
	r, err = ipv6.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b", Address: advertised})
	registered, ok, _ := coordinatorWorkerSnapshot(f.s, "worker-b")
	if err != nil || !r.GetAccepted() || !ok || registered.Address != advertised {
		t.Fatal("certificate-bound IPv6 advertisement rejected or rewritten")
	}
	// The direct business boundary also refuses resolver targets.
	if r, err := f.s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: "new-worker", Address: "unix:///tmp/socket"}); err != nil || r.GetAccepted() {
		t.Fatal("direct registration accepted a resolver target")
	}
}

func TestMTLSReregistrationCannotBypassDispatchIdentity(t *testing.T) {
	f := newMTLSCoordinator(t)
	var wrongCalls atomic.Int32
	first := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	worker := registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", first)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	ctx := mtlsContext(t)
	if r, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 11104, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}); err != nil || r.GetStatus() != "Succeeded" {
		t.Fatal("initial verified endpoint failed")
	}
	other := startMTLSBackend(t, f, workerBIdentity, func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		wrongCalls.Add(1)
		return successfulResult(r), nil
	})
	if r, err := worker.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: other}); err != nil || !r.GetAccepted() {
		t.Fatal("legitimate same-host endpoint update rejected")
	}
	req := &runtimepb.SubmitJobRequest{JobId: 11105, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
	if r, err := client.SubmitJob(ctx, req); err == nil || r != nil || wrongCalls.Load() != 0 {
		t.Fatal("endpoint replacement changed the expected logical identity")
	}
	record, ok, err := f.store.Load(11105)
	if err != nil || !ok || record.State != JobFailed || record.AttemptID != 2 {
		t.Fatal("mismatched endpoint did not remain an infrastructure failure")
	}
	replacement := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	if r, err := worker.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: replacement}); err != nil || !r.GetAccepted() {
		t.Fatal(err)
	}
	r, err := client.SubmitJob(ctx, req)
	if err != nil || r.GetStatus() != "Succeeded" || r.GetAttemptId() != 3 {
		t.Fatalf("verified replacement did not continue attempt generation: %v", err)
	}
	_, _, order := coordinatorWorkerSnapshot(f.s, "worker-a")
	if !reflect.DeepEqual(order, []string{"worker-a"}) || wrongCalls.Load() != 0 {
		t.Fatal("re-registration duplicated order or accepted the wrong Worker")
	}
}

func TestMTLSTimedRecoveryAfterProvingCapacityReturns(t *testing.T) {
	f := newMTLSCoordinator(t, coordinatorExecutionConfig{ListenAddress: "127.0.0.1:0", AttemptLease: 45 * time.Second, ShutdownTimeout: defaultShutdownTimeout})
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	record := preimageRecord(11106, 7, "old-worker", JobRecovering, fixture.payload)
	saveRecoveryRecord(t, f.store, record)
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy proving capacity")
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(limiter.Release) })
	proving := &admissionProvingMetrics{}
	var calls atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, limitedPreimageWorker(fixture, limiter, proving, &calls))
	worker := registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	terminal := make(chan struct{}, 1)
	var accepted atomic.Bool
	f.observer.hook = func(e recordedMetric) {
		if e.name == "terminal" {
			accepted.Store(true)
		}
		if e.name == "pass" && accepted.Swap(false) {
			terminal <- struct{}{}
		}
	}
	c := newManualRecoveryClock()
	q := testRecoverySchedule(c)
	stop := startManualRecovery(t, f.s, q, c)
	waitRecoveryReset(t, c, time.Second)
	want := record
	want.AttemptID = 9
	want.WorkerID = "worker-a"
	assertRecoveryRecord(t, f.store, want)
	if calls.Load() != 2 || proving.samples.Load() != 0 || f.observer.count("terminal", "") != 0 {
		t.Fatal("busy recovery did not retain two-attempt infrastructure failure semantics")
	}
	// Readiness still cannot bypass cooldown, including authenticated revival.
	f.s.mu.Lock()
	w := f.s.workers["worker-a"]
	w.Status = WorkerDead
	f.s.workers[w.ID] = w
	f.s.mu.Unlock()
	if r, err := worker.Heartbeat(mtlsContext(t), &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}); err != nil || !r.GetAccepted() {
		t.Fatal(err)
	}
	waitRecoveryReset(t, c, time.Second)
	release.Do(limiter.Release)
	beforeRetry := time.Now()
	c.advance(time.Second)
	waitRecoverySignal(t, terminal)
	stop()
	want.State = JobSucceeded
	want.AttemptID = 10
	assertRecoveryRecord(t, f.store, want)
	lease, ok := f.s.getLease(record.JobID)
	if !ok || lease.AttemptID != 10 || lease.WorkerID != "worker-a" || calls.Load() != 3 || proving.samples.Load() != 1 || f.observer.count("verification", "") != 1 || f.observer.count("submitted", "") != 0 || len(q.entries) != 0 {
		t.Fatal("timed recovery changed identity, generation, proof verification, lease or metrics")
	}
	if lease.ExpiresAt.Before(beforeRetry.Add(45*time.Second)) || lease.ExpiresAt.After(time.Now().Add(45*time.Second)) {
		t.Fatal("recovery did not use the configured attempt lease")
	}
	assertRecoveryOwnershipReleased(t, f.s, record.JobID)
}

func TestMTLSDispatchFailsClosedWithoutFactory(t *testing.T) {
	s := &CoordinatorServer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if r, err := s.dispatchJob(ctx, WorkerInfo{ID: "worker-a", Address: "localhost:1234"}, &runtimepb.ExecuteJobRequest{}); r != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing factory did not fail closed: %v", err)
	}
	if r, err := s.dispatchJob(ctx, WorkerInfo{ID: "worker-a", Address: "unix:///tmp/socket"}, &runtimepb.ExecuteJobRequest{}); r != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("resolver target reached dispatch")
	}
	// Port parsing also rejects numeric resolver aliases while retaining normal
	// bracketed IPv6 without DNS or network lookups.
	if host, err := rpcauth.ParseTCPEndpoint("[::1]:" + strconv.Itoa(1234)); err != nil || host != "::1" {
		t.Fatal("IPv6 endpoint not preserved")
	}
	if _, err := rpcauth.ParseTCPEndpoint(strings.Repeat("a", 64) + ":1234"); err == nil {
		t.Fatal("oversized DNS label accepted")
	}
}
