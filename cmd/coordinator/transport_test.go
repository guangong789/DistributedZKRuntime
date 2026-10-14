package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"maps"
	"net"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const workerBIdentity = "spiffe://distributedzk.local/worker/worker-b"

type mtlsCoordinator struct {
	ca        *testpki.CA
	s         *CoordinatorServer
	store     *workerFailureStore
	observer  *recordingMetrics
	address   string
	server    *grpc.Server
	serveDone chan struct{}
}

func newMTLSCoordinator(t *testing.T, executionOptions ...coordinatorExecutionConfig) *mtlsCoordinator {
	t.Helper()
	execution := coordinatorExecutionConfig{ListenAddress: "127.0.0.1:0", AttemptLease: defaultAttemptLease, ShutdownTimeout: defaultShutdownTimeout}
	if len(executionOptions) != 0 {
		execution = executionOptions[0]
	}
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Coordinator, nil)
	cfg := coordinatorSecurityConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, Identity: testpki.Coordinator, Clients: []string{testpki.Client}, Workers: []string{testpki.Worker, workerBIdentity}}
	transport, err := cfg.load()
	if err != nil {
		t.Fatal(err)
	}
	store := newWorkerFailureStore(t)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, metrics: observer, recoveryReady: make(chan struct{}, 1), workerCredentials: transport.workerCredentials, leaseDuration: execution.AttemptLease}
	listener, err := execution.listen()
	if err != nil {
		t.Fatal(err)
	}
	server := transport.newServer(s)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return &mtlsCoordinator{ca: ca, s: s, store: store, observer: observer,
		address: listener.Addr().String(), server: server, serveDone: done}
}

func (f *mtlsCoordinator) connection(t *testing.T, identity string) *grpc.ClientConn {
	t.Helper()
	files := f.ca.Issue(t, identity, nil)
	local, err := rpcauth.ParsePrincipal(identity)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := (rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}).Credentials(f.address, local)
	if err != nil {
		t.Fatal(err)
	}
	return f.connect(t, transport)
}
func (f *mtlsCoordinator) connect(t *testing.T, transport credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(transport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
func mtlsContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func startMTLSBackend(t *testing.T, f *mtlsCoordinator, identity string, execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	files := f.ca.Issue(t, identity, nil)
	local, err := rpcauth.ParsePrincipal(identity)
	if err != nil {
		t.Fatal(err)
	}
	material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, local)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	server, err := rpcauth.NewWorkerServer(material, coordinator, &testWorker{execute: execute})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String()
}

type mtlsSnapshot struct {
	workers       map[string]WorkerInfo
	order         []string
	cursor        int
	attempts      map[int64]int64
	leases        map[int64]JobLease
	active        map[int64]struct{}
	notifications int
	saves         []JobRecord
	events        []recordedMetric
}

func (f *mtlsCoordinator) snapshot() mtlsSnapshot {
	f.s.mu.Lock()
	snap := mtlsSnapshot{workers: maps.Clone(f.s.workers), order: append([]string(nil), f.s.workerOrder...), cursor: f.s.nextWorker,
		attempts: maps.Clone(f.s.jobAttempts), leases: maps.Clone(f.s.leases), active: maps.Clone(f.s.activeJobs), notifications: len(f.s.recoveryReady)}
	f.s.mu.Unlock()
	snap.saves = f.store.savedRecords()
	snap.events = f.observer.snapshot()
	return snap
}
func (f *mtlsCoordinator) assertUnchanged(t *testing.T, before mtlsSnapshot, jobID int64) {
	t.Helper()
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("rejected RPC changed registry, LastSeen, scheduling, attempts, leases, ownership, SQLite writes, recovery notifications or metrics")
	}
	if _, ok, err := f.store.Load(jobID); err != nil || ok {
		t.Fatal("rejected RPC created a durable job")
	}
}

func TestCoordinatorMTLSAuthorizedPaths(t *testing.T) {
	f := newMTLSCoordinator(t)
	zk := newIntegrationZKFixture(t)
	f.s.preimageVerifier = zk.verifier
	var executions atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		executions.Add(1)
		return zk.execute(ctx, r, nil)
	})
	workerConn := f.connection(t, testpki.Worker)
	workerClient := &recordingCoordinatorClient{CoordinatorServiceClient: runtimepb.NewCoordinatorServiceClient(workerConn)}
	ctx := mtlsContext(t)
	// Unknown but authorized Worker recovers using the same client/connection.
	if err := workerheartbeat.Send(ctx, workerClient, "worker-a", address); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(workerClient.heartbeatAccepted, []bool{false}) || len(workerClient.registrations) != 1 {
		t.Fatal("unknown heartbeat did not trigger exactly one re-registration")
	}
	if err := workerheartbeat.Send(ctx, workerClient, "worker-a", address); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(workerClient.heartbeatAccepted, []bool{false, true}) || len(workerClient.registrations) != 1 {
		t.Fatal("accepted heartbeat re-registered")
	}
	w, ok, order := coordinatorWorkerSnapshot(f.s, "worker-a")
	if !ok || w.Status != WorkerAlive || w.Address != address || w.LastSeen.IsZero() || !reflect.DeepEqual(order, []string{"worker-a"}) {
		t.Fatal("worker recovery did not publish one alive scheduling entry")
	}
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	response, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 10001, TaskType: "zk_preimage_prove", Payload: zk.payload, TimeoutMs: 2000})
	if err != nil || response.GetStatus() != "Succeeded" || response.GetAttemptId() != 1 || executions.Load() != 1 {
		t.Fatalf("authorized submission failed: %v", err)
	}
	record, ok, err := f.store.Load(10001)
	if err != nil || !ok || record.State != JobSucceeded || record.AttemptID != 1 || record.WorkerID != "worker-a" || record.Payload != zk.payload {
		t.Fatal("authenticated ZK completion was not verified/persisted correctly")
	}
	if f.observer.count("verification", "") != 1 || f.observer.count("submitted", "") != 1 || !f.s.claimJob(10001) {
		t.Fatal("verification/metrics/ownership semantics changed")
	}
	f.s.releaseJob(10001)
	// Same authenticated identity may update its endpoint without another order entry.
	newAddress := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	registered, err := workerClient.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: newAddress})
	if err != nil || !registered.GetAccepted() {
		t.Fatal(err)
	}
	selected, ok := f.s.getNextWorker()
	_, _, order = coordinatorWorkerSnapshot(f.s, "worker-a")
	if !ok || selected.Address != newAddress || !reflect.DeepEqual(order, []string{"worker-a"}) {
		t.Fatal("re-registration changed endpoint/order/scheduling semantics")
	}
}

func TestCoordinatorMTLSRejectsRolesAndWorkerIDWithoutSideEffects(t *testing.T) {
	f := newMTLSCoordinator(t)
	var dispatches atomic.Int32
	registerTestWorker(t, f.s, "worker-a", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		dispatches.Add(1)
		return successfulResult(r), nil
	})
	f.s.mu.Lock()
	f.s.workers["worker-b"] = WorkerInfo{ID: "worker-b", Address: "protected-endpoint", Status: WorkerDead, LastSeen: time.Unix(1, 0)}
	f.s.workerOrder = append(f.s.workerOrder, "worker-b")
	f.s.mu.Unlock()
	for _, tc := range []struct {
		name, caller, method string
		request, response    any
	}{
		{"Client cannot register", testpki.Client, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b", Address: "attacker"}, &runtimepb.RegisterWorkerResponse{}},
		{"Client cannot heartbeat", testpki.Client, runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-b"}, &runtimepb.HeartbeatResponse{}},
		{"Worker cannot submit", testpki.Worker, runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{JobId: 10002, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}, &runtimepb.SubmitJobResponse{}},
		{"Worker cannot register another ID", testpki.Worker, runtimepb.CoordinatorService_RegisterWorker_FullMethodName, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b", Address: "attacker"}, &runtimepb.RegisterWorkerResponse{}},
		{"Worker cannot heartbeat another ID", testpki.Worker, runtimepb.CoordinatorService_Heartbeat_FullMethodName, &runtimepb.HeartbeatRequest{WorkerId: "worker-b"}, &runtimepb.HeartbeatResponse{}},
		{"unknown method", testpki.Client, "/runtime.CoordinatorService/Unknown", &runtimepb.SubmitJobRequest{}, &runtimepb.SubmitJobResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := f.connection(t, tc.caller)
			before := f.snapshot()
			ctx := metadata.NewOutgoingContext(mtlsContext(t), metadata.Pairs("role", "worker", "identity", workerBIdentity, "worker-id", "worker-b"))
			if err := conn.Invoke(ctx, tc.method, tc.request, tc.response); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("error=%v", err)
			}
			f.assertUnchanged(t, before, 10002)
			if dispatches.Load() != 0 {
				t.Fatal("rejected SubmitJob dispatched")
			}
		})
	}
}

func TestCoordinatorMTLSRejectsTLSFailuresWithoutSideEffects(t *testing.T) {
	f := newMTLSCoordinator(t)
	f.s.workers = map[string]WorkerInfo{"worker-a": {ID: "worker-a", Address: "protected-endpoint", Status: WorkerDead, LastSeen: time.Unix(1, 0)}}
	f.s.workerOrder = []string{"worker-a"}
	clientCert := f.ca.Issue(t, testpki.Client, nil)
	for _, tc := range []struct {
		name         string
		cert         func() *tls.Certificate
		nameOverride string
		plaintext    bool
	}{
		{name: "missing certificate", cert: func() *tls.Certificate { return nil }},
		{name: "untrusted CA", cert: func() *tls.Certificate { v := testpki.New(t).Issue(t, testpki.Client, nil); return &v.Pair }},
		{name: "expired", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { c.NotAfter = time.Now().Add(-time.Minute) })
			return &v.Pair
		}},
		{name: "future", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { c.NotBefore = time.Now().Add(time.Minute) })
			return &v.Pair
		}},
		{name: "wrong EKU", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} })
			return &v.Pair
		}},
		{name: "missing URI", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { c.URIs = nil })
			return &v.Pair
		}},
		{name: "ambiguous URI", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { c.URIs = append(c.URIs, c.URIs[0]) })
			return &v.Pair
		}},
		{name: "malformed identity", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, testpki.Client, func(c *x509.Certificate) { u, _ := url.Parse(testpki.Client + "?role=worker"); c.URIs = []*url.URL{u} })
			return &v.Pair
		}},
		{name: "unallowlisted Client", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, "spiffe://distributedzk.local/client/unknown", nil)
			return &v.Pair
		}},
		{name: "unallowlisted Worker", cert: func() *tls.Certificate {
			v := f.ca.Issue(t, "spiffe://distributedzk.local/worker/unknown", nil)
			return &v.Pair
		}},
		{name: "wrong DNS SAN", cert: func() *tls.Certificate { return &clientCert.Pair }, nameOverride: "wrong.example"},
		{name: "plaintext", plaintext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var transport credentials.TransportCredentials
			if tc.plaintext {
				transport = insecure.NewCredentials()
			} else {
				cert := tc.cert()
				config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: f.ca.Roots(), ServerName: "127.0.0.1"}
				if tc.nameOverride != "" {
					config.ServerName = tc.nameOverride
				}
				if cert != nil {
					config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
				}
				transport = credentials.NewTLS(config)
			}
			client := runtimepb.NewCoordinatorServiceClient(f.connect(t, transport))
			before := f.snapshot()
			if response, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 10003, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}); err == nil || response != nil {
				t.Fatal("invalid TLS peer invoked Coordinator")
			}
			if response, err := client.RegisterWorker(mtlsContext(t), &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: "attacker"}); err == nil || response != nil {
				t.Fatal("invalid TLS peer reached registration")
			}
			if response, err := client.Heartbeat(mtlsContext(t), &runtimepb.HeartbeatRequest{WorkerId: "worker-a"}); err == nil || response != nil {
				t.Fatal("invalid TLS peer reached heartbeat")
			}
			f.assertUnchanged(t, before, 10003)
		})
	}
}

func TestCoordinatorMTLSAuthenticatedPayloadValidation(t *testing.T) {
	f := newMTLSCoordinator(t)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	before := f.snapshot()
	response, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 10004, TaskType: "zk_preimage_prove", Payload: `{"witness_ref":"ref","digest":"1","secret":"PRIVATE_MARKER"}`, TimeoutMs: 1000})
	if response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("payload validation after authentication: %v", err)
	}
	f.assertUnchanged(t, before, 10004)
}

func TestCoordinatorMTLSConcurrentHeartbeatRevival(t *testing.T) {
	f := newMTLSCoordinator(t)
	old := WorkerInfo{ID: "worker-a", Address: "unchanged", Status: WorkerDead, LastSeen: time.Unix(1, 0)}
	f.s.workers = map[string]WorkerInfo{old.ID: old}
	f.s.workerOrder = []string{old.ID}
	f.s.recoveryReady = make(chan struct{}, 32)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	ctx := mtlsContext(t)
	var wg sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: old.ID})
			if err != nil {
				failures <- err
			} else if !resp.GetAccepted() {
				failures <- status.Error(codes.Internal, "heartbeat rejected")
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("heartbeat revival blocked")
	}
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	w, ok, order := coordinatorWorkerSnapshot(f.s, old.ID)
	if !ok || w.Status != WorkerAlive || !w.LastSeen.After(old.LastSeen) || w.Address != old.Address || len(order) != 1 || len(f.s.recoveryReady) != 1 || f.observer.count("worker", "dead/alive/heartbeat") != 1 || f.observer.count("worker", "") != 1 {
		t.Fatal("authenticated concurrent revival changed transition/notification semantics")
	}
}

func TestCoordinatorMTLSConfigurationFailsClosed(t *testing.T) {
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Coordinator, nil)
	valid := coordinatorSecurityConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, Identity: testpki.Coordinator, Clients: []string{testpki.Client}, Workers: []string{testpki.Worker}}
	for _, tc := range []struct {
		name   string
		change func(*coordinatorSecurityConfig)
	}{
		{"missing cert", func(c *coordinatorSecurityConfig) { c.CertFile = "" }}, {"missing key", func(c *coordinatorSecurityConfig) { c.KeyFile = "" }}, {"missing CA", func(c *coordinatorSecurityConfig) { c.CAFile = "" }},
		{"unreadable cert", func(c *coordinatorSecurityConfig) { c.CertFile = "does-not-exist" }},
		{"invalid identity", func(c *coordinatorSecurityConfig) { c.Identity = testpki.Worker }},
		{"local certificate mismatch", func(c *coordinatorSecurityConfig) { c.CertFile = ca.Issue(t, testpki.Worker, nil).Cert }},
		{"wrong key", func(c *coordinatorSecurityConfig) { c.KeyFile = ca.Issue(t, testpki.Coordinator, nil).Key }},
		{"wildcard Client", func(c *coordinatorSecurityConfig) { c.Clients = []string{"*"} }},
		{"empty entry", func(c *coordinatorSecurityConfig) { c.Clients = []string{""} }},
		{"Client role escalation", func(c *coordinatorSecurityConfig) { c.Clients = []string{testpki.Worker} }},
		{"Worker role escalation", func(c *coordinatorSecurityConfig) { c.Workers = []string{testpki.Client} }},
		{"duplicate entries", func(c *coordinatorSecurityConfig) { c.Workers = []string{testpki.Worker, testpki.Worker} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.change(&c)
			transport, err := c.load()
			if err == nil || transport != nil {
				t.Fatal("invalid startup credentials/policy accepted")
			}
		})
	}
	// Empty allowlists are valid default-deny policy, never an implicit wildcard.
	empty := valid
	empty.Clients = nil
	empty.Workers = nil
	transport, err := empty.load()
	if err != nil {
		t.Fatal(err)
	}
	p, _ := rpcauth.ParsePrincipal(testpki.Client)
	if err := transport.policy.AuthorizeRPC(p, runtimepb.CoordinatorService_SubmitJob_FullMethodName, &runtimepb.SubmitJobRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("empty allowlist granted access")
	}
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	cfg := coordinatorSecurityFlags(fs)
	if err := fs.Parse([]string{"-tls-cert", files.Cert, "-tls-key", files.Key, "-tls-ca", files.CA, "-allow-client", testpki.Client, "-allow-worker", testpki.Worker}); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.load(); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorMTLSRetryFencingAndLease(t *testing.T) {
	for _, leaseExpiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "mismatched result fenced", true: "lease expiry retries child only"}[leaseExpiry], func(t *testing.T) {
			f := newMTLSCoordinator(t)
			if leaseExpiry {
				f.s.leaseDuration = 250 * time.Millisecond
			}
			firstCanceled := make(chan struct{})
			var firstCalls, secondCalls atomic.Int32
			firstAddress := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				firstCalls.Add(1)
				if r.AttemptId != 1 {
					t.Error("first Worker did not receive attempt 1")
				}
				if leaseExpiry {
					<-ctx.Done()
					close(firstCanceled)
					return nil, ctx.Err()
				}
				response := successfulResult(r)
				response.AttemptId = 0
				return response, nil
			})
			secondAddress := startMTLSBackend(t, f, workerBIdentity, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				secondCalls.Add(1)
				if r.AttemptId != 2 || ctx.Err() != nil {
					t.Error("retry did not receive a fresh live attempt 2")
				}
				return successfulResult(r), nil
			})
			ctx := mtlsContext(t)
			for _, worker := range []struct{ identity, id, address string }{{testpki.Worker, "worker-a", firstAddress}, {workerBIdentity, "worker-b", secondAddress}} {
				client := runtimepb.NewCoordinatorServiceClient(f.connection(t, worker.identity))
				r, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: worker.id, Address: worker.address})
				if err != nil || !r.GetAccepted() {
					t.Fatal(err)
				}
			}
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			response, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 10005, TaskType: "hash", Payload: "hello", TimeoutMs: 2000})
			if err != nil || response.GetStatus() != "Succeeded" || response.GetAttemptId() != 2 || firstCalls.Load() != 1 || secondCalls.Load() != 1 || ctx.Err() != nil {
				t.Fatalf("authenticated retry changed: %v", err)
			}
			if leaseExpiry {
				select {
				case <-firstCanceled:
				case <-ctx.Done():
					t.Fatal("expired attempt was not canceled")
				}
			}
			writes := f.store.savedRecords()
			if len(writes) != 3 || writes[0].State != JobRunning || writes[0].AttemptID != 1 || writes[1].State != JobRunning || writes[1].AttemptID != 2 || writes[2].State != JobSucceeded || writes[2].AttemptID != 2 || writes[2].WorkerID != "worker-b" {
				t.Fatal("fenced or expired attempt overwrote terminal durable state")
			}
			lease, ok := f.s.getLease(10005)
			if !ok || lease.AttemptID != 2 || lease.WorkerID != "worker-b" {
				t.Fatal("lease did not track accepted attempt")
			}
			assertRecoveryOwnershipReleased(t, f.s, 10005)
		})
	}
}

func TestCoordinatorMTLSOwnershipConflict(t *testing.T) {
	f := newMTLSCoordinator(t)
	if !f.s.claimJob(10006) {
		t.Fatal("cannot preclaim")
	}
	defer f.s.releaseJob(10006)
	before := f.snapshot()
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	response, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 10006, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	if response != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("authenticated submission bypassed ownership: %v", err)
	}
	f.assertUnchanged(t, before, 10006)
}

func TestCoordinatorMTLSCancellationPersistsWithoutRetry(t *testing.T) {
	f := newMTLSCoordinator(t)
	started := make(chan struct{})
	canceledSaved := make(chan struct{})
	var calls atomic.Int32
	f.observer.hook = func(e recordedMetric) {
		if e.name == "terminal" && e.label == "cancelled" {
			close(canceledSaved)
		}
	}
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	wait := mtlsContext(t)
	worker := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	if r, err := worker.RegisterWorker(wait, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: address}); err != nil || !r.GetAccepted() {
		t.Fatal(err)
	}
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	ctx, cancel := context.WithCancel(wait)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 10007, TaskType: "hash", Payload: "hello", TimeoutMs: 2000})
		result <- err
	}()
	select {
	case <-started:
	case <-wait.Done():
		t.Fatal("attempt did not start")
	}
	cancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("parent cancellation: %v", err)
		}
	case <-wait.Done():
		t.Fatal("client cancellation did not finish")
	}
	// A canceled gRPC client may return before the server finishes cancellation
	// persistence. Wait for the post-Save semantic event instead of sleeping.
	select {
	case <-canceledSaved:
	case <-wait.Done():
		t.Fatal("server did not durably finalize cancellation")
	}
	record, ok, err := f.store.Load(10007)
	if err != nil || !ok || record.State != JobCancelled || record.AttemptID != 1 || record.WorkerID != "worker-a" || calls.Load() != 1 || f.observer.count("started", "") != 1 {
		t.Fatal("authenticated cancellation did not preserve no-retry durable semantics")
	}
}
