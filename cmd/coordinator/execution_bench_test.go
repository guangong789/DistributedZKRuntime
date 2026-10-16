package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	jobruntime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

const (
	submitBenchWarmup    = 3
	submitBenchTaskMs    = 30000
	submitBenchLease     = 45 * time.Second
	submitBenchRPCBudget = 120 * time.Second
	submitBenchSetup     = 15 * time.Second
	submitBenchCleanup   = 10 * time.Second
	submitBenchWorkerID  = "worker-a"
)

// This fixture uses production Coordinator transport, admission, execution and
// P1 cleanup. No liveness/recovery loops or heartbeat producer are started: this
// is a healthy single-endpoint request baseline, not a process/recovery test.
// The Worker uses production rpcauth authorization and the existing real Task /
// Runtime handler, not cmd/worker.WorkerService or its lifecycle/logging wrapper.
type submitBenchmarkFixture struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	process             *coordinatorProcess
	store               *SQLiteJobStore
	workers             []*submitBenchmarkWorker
	connections         []*grpc.ClientConn
	client              runtimepb.CoordinatorServiceClient
	coordinatorRegistry *prom.Registry
	proofs              capacityProofCounts
	keyHashes           map[string]string
	origin              time.Time
	beforeClose         func() error
	finishOnce          sync.Once
	finishErr           error
	cleanup             capacityCleanup
	payload             string
	sqlite              map[string]any
}

func newSubmitBenchmarkFixture(b *testing.B) *submitBenchmarkFixture {
	return newConfiguredSubmitBenchmarkFixture(b, submitBenchmarkConfig{workers: 1, capacity: 1})
}

func newConfiguredSubmitBenchmarkFixture(b testing.TB, cfg submitBenchmarkConfig) *submitBenchmarkFixture {
	b.Helper()
	if cfg.workers < 1 || cfg.workers > 2 || cfg.capacity < 1 || cfg.capacity > 2 {
		b.Fatal("invalid bounded benchmark configuration")
	}
	databaseFile := filepath.Join(b.TempDir(), "jobs.db")
	f := &submitBenchmarkFixture{origin: time.Now()}
	f.ctx, f.cancel = context.WithCancel(context.Background())
	b.Cleanup(func() {
		if err := f.finish(); err != nil {
			b.Errorf("benchmark cleanup: %v", err)
		}
	})
	var crypto integrationZKFixture
	if cfg.keyDir == "" {
		if cfg.workers != 1 {
			b.Fatal("multiple Workers require a common setup directory")
		}
		crypto = newIntegrationZKFixture(b)
	} else {
		crypto, f.keyHashes = loadCapacityCrypto(b, cfg.keyDir)
	}
	f.payload = crypto.payload
	ca := testpki.New(b)
	coordinatorFiles := ca.Issue(b, testpki.Coordinator, nil)
	ids := []string{"worker-a", "worker-b"}[:cfg.workers]
	identities := make([]string, len(ids))
	for i, id := range ids {
		identities[i] = "spiffe://distributedzk.local/worker/" + id
	}
	transport, err := (coordinatorSecurityConfig{
		CertFile: coordinatorFiles.Cert, KeyFile: coordinatorFiles.Key, CAFile: coordinatorFiles.CA,
		Identity: testpki.Coordinator, Clients: []string{testpki.Client}, Workers: identities,
	}).load()
	if err != nil {
		b.Fatal(err)
	}
	f.coordinatorRegistry = prom.NewRegistry()
	coordinatorMetrics, err := prommetrics.New(f.coordinatorRegistry)
	if err != nil {
		b.Fatal(err)
	}
	f.store, err = NewSQLiteJobStore(databaseFile)
	if err != nil {
		b.Fatal(err)
	}
	s := &CoordinatorServer{jobStore: f.store, metrics: coordinatorMetrics, preimageVerifier: crypto.verifier,
		workerCredentials: transport.workerCredentials, leaseDuration: submitBenchLease}
	f.process = newCoordinatorProcess(s, submitBenchCleanup)
	f.process.closeStore = func() error {
		var auditErr error
		if f.beforeClose != nil {
			auditErr = f.beforeClose()
		}
		return errors.Join(auditErr, f.store.Close())
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	f.process.server, f.process.listener = transport.newServer(s), listener
	f.process.serveDone = make(chan struct{})
	go func() {
		defer close(f.process.serveDone)
		if err := f.process.server.Serve(listener); err != nil {
			b.Errorf("Coordinator Serve: %v", err)
		}
	}()
	connect := func(files testpki.Files, principal rpcauth.Principal) *grpc.ClientConn {
		creds, err := (rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key,
			CAFile: files.CA, ServerIdentity: testpki.Coordinator}).Credentials(listener.Addr().String(), principal)
		if err != nil {
			b.Fatal(err)
		}
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(creds))
		if err != nil {
			b.Fatal(err)
		}
		f.connections = append(f.connections, conn)
		readyCtx, cancel := context.WithTimeout(f.ctx, submitBenchSetup)
		defer cancel()
		conn.Connect()
		for state := conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
			if state == connectivity.Shutdown || !conn.WaitForStateChange(readyCtx, state) {
				b.Fatal("Coordinator mTLS connection did not become Ready within startup budget")
			}
		}
		return conn
	}

	coordinatorPrincipal, err := rpcauth.ParsePrincipal(testpki.Coordinator)
	if err != nil {
		b.Fatal(err)
	}
	for i, id := range ids {
		w := &submitBenchmarkWorker{id: id, registry: prom.NewRegistry()}
		if cfg.track {
			w.events = make(chan capacityEvent, 4096)
		}
		f.workers = append(f.workers, w)
		observer, err := prommetrics.New(w.registry)
		if err != nil {
			b.Fatal(err)
		}
		w.limiter, err = jobruntime.NewProvingLimiter(cfg.capacity)
		if err != nil {
			b.Fatal(err)
		}
		localCrypto := crypto
		if i > 0 {
			localCrypto.prover, err = zk.LoadPreimageProver(filepath.Join(cfg.keyDir, "proving.key"))
			if err != nil {
				b.Fatal(err)
			}
			witnesses := jobruntime.NewMemoryWitnessStore()
			witnesses.Put("secret-001", 42)
			localCrypto.witnesses = witnesses
		}
		w.prover, w.witnesses = localCrypto.prover, localCrypto.witnesses
		files := ca.Issue(b, identities[i], nil)
		principal, err := rpcauth.ParsePrincipal(identities[i])
		if err != nil {
			b.Fatal(err)
		}
		material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, principal)
		if err != nil {
			b.Fatal(err)
		}
		var factory []func(context.Context, *runtimepb.ExecuteJobRequest) jobruntime.PreimageProver
		if cfg.track {
			factory = append(factory, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) jobruntime.PreimageProver {
				return &capacityTrackedProver{ctx: ctx, delegate: localCrypto.prover, worker: w, all: &f.proofs, origin: f.origin, jobID: req.JobId, attemptID: req.AttemptId}
			})
		}
		execute := limitedPreimageWorker(localCrypto, w.limiter, observer, &w.calls, factory...)
		w.server, err = rpcauth.NewWorkerServer(material, coordinatorPrincipal, &testWorker{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			if _, err := jobruntime.TaskTimeoutFromMilliseconds(req.TimeoutMs); err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid task timeout")
			}
			resp, err := execute(ctx, req)
			if status.Code(err) == codes.ResourceExhausted {
				w.busy.Add(1)
			}
			if cfg.track {
				w.record(capacityEvent{Kind: "rpc", JobID: req.JobId, AttemptID: req.AttemptId, Code: status.Code(err).String()})
			}
			return resp, err
		}})
		if err != nil {
			b.Fatal(err)
		}
		workerListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		w.done = make(chan struct{})
		go func() {
			defer close(w.done)
			if err := w.server.Serve(workerListener); err != nil {
				b.Errorf("Worker %s Serve: %v", w.id, err)
			}
		}()
		registration := runtimepb.NewCoordinatorServiceClient(connect(files, principal))
		setupCtx, cancel := context.WithTimeout(f.ctx, submitBenchSetup)
		registered, err := registration.RegisterWorker(setupCtx, &runtimepb.RegisterWorkerRequest{WorkerId: id, Address: workerListener.Addr().String()}, grpc.WaitForReady(true))
		cancel()
		if err != nil || registered == nil || !registered.Accepted {
			b.Fatal("authenticated Worker registration failed")
		}
	}
	// Exercise the production selector once per registered Worker; a full cycle
	// leaves the cursor unchanged for the first warmup job.
	for _, id := range ids {
		w, ok := s.getNextWorker()
		if !ok || w.ID != id || w.Status != WorkerAlive {
			b.Fatal("Worker is not Alive/selectable after registration")
		}
	}
	clientPrincipal, err := rpcauth.ParsePrincipal(testpki.Client)
	if err != nil {
		b.Fatal(err)
	}
	f.client = runtimepb.NewCoordinatorServiceClient(connect(ca.Issue(b, testpki.Client, nil), clientPrincipal))
	setupCtx, cancel := context.WithTimeout(f.ctx, submitBenchSetup)
	defer cancel()
	// Read actual SQLite settings without tuning pragmas. Same file grows across
	// warmup + measured requests; each benchmark invocation starts a fresh DB.
	var journal string
	var synchronous int
	if err := f.store.db.QueryRowContext(setupCtx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		b.Fatal(err)
	}
	if err := f.store.db.QueryRowContext(setupCtx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		b.Fatal(err)
	}
	var databasePath string
	var sequence int
	var databaseName string
	if err := f.store.db.QueryRowContext(setupCtx, "PRAGMA database_list").Scan(&sequence, &databaseName, &databasePath); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(databasePath)
	if err != nil || databasePath == "" || !info.Mode().IsRegular() {
		b.Fatal("SQLite fixture is not a regular file-backed database")
	}
	f.sqlite = map[string]any{"file_backed": true, "file_mode": info.Mode().String(), "journal_mode": journal, "synchronous": synchronous}
	return f
}

// Identity fields are attached outside the timed path. Flattened JSON/CSV rows
// must retain (run_id, invocation, cohort, job_id); job_id alone is fixture-local.
type submitBenchmarkSample struct {
	RunID           string `json:"run_id"`
	Invocation      string `json:"invocation"`
	Cohort          string `json:"cohort"`
	Iteration       int    `json:"iteration"`
	JobID           int64  `json:"job_id"`
	AttemptID       int64  `json:"attempt_id"`
	RPCNanoseconds  int64  `json:"rpc_ns"`
	RPCCode         string `json:"rpc_code"`
	ResponseState   string `json:"response_state"`
	DurableState    string `json:"durable_state"`
	DurableAttempt  int64  `json:"durable_attempt_id"`
	MultipleAttempt bool   `json:"multiple_attempts"`
	Failure         string `json:"failure,omitempty"`
}

type submitBenchmarkMetric struct {
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels,omitempty"`
	Value      float64           `json:"counter_delta,omitempty"`
	Count      uint64            `json:"sample_count_delta,omitempty"`
	SumSeconds float64           `json:"sum_seconds_delta,omitempty"`
}

func submitBenchmarkMetrics(before, after integrationMetricsView) []submitBenchmarkMetric {
	names := make([]string, 0, len(after))
	for name := range after {
		names = append(names, name)
	}
	sort.Strings(names)
	var deltas []submitBenchmarkMetric
	for _, name := range names {
		for _, sample := range after[name] {
			previous := before.sample(name, sample.labels)
			deltas = append(deltas, submitBenchmarkMetric{Name: name, Labels: sample.labels,
				Value: sample.value - previous.value, Count: sample.count - previous.count, SumSeconds: sample.sum - previous.sum})
		}
	}
	return deltas
}

// BenchmarkSubmitJobEndToEnd measures one sequential client's warm connection
// over mTLS to the production Coordinator, real per-attempt mTLS dispatch, Task /
// Runtime proving, Coordinator verification and SQLite terminal Save. Requests,
// per-request contexts/cancel, RPC timestamping and light response checks are
// timed. The independent post-response SQLite Load, metrics Gather and JSON
// reporting are excluded. B/op is whole-process allocation during timed windows,
// not just client allocation. All cohorts (including Go calibration) are printed
// separately as P3B1_RESULT JSON with their own invocation ID and b.N. Warmup
// rows are separate; measured rows belong only to that invocation. Select the
// final invocation using the command manifest/iteration count, not JobID.
func BenchmarkSubmitJobEndToEnd(b *testing.B) {
	if b.N > 1000 {
		b.Fatal("bounded P3-B1 baseline: use -benchtime=20x (maximum 1000 jobs per invocation)")
	}
	invocation := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	f := newSubmitBenchmarkFixture(b)
	samples := make([]submitBenchmarkSample, 0, b.N)
	run := func(jobID int64) submitBenchmarkSample {
		request := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "zk_preimage_prove", Payload: f.payload, TimeoutMs: submitBenchTaskMs}
		ctx, cancel := context.WithTimeout(f.ctx, submitBenchRPCBudget)
		start := time.Now()
		response, err := f.client.SubmitJob(ctx, request)
		elapsed := time.Since(start)
		cancel()
		sample := submitBenchmarkSample{JobID: jobID, RPCNanoseconds: elapsed.Nanoseconds(), RPCCode: status.Code(err).String()}
		if response != nil {
			sample.AttemptID, sample.ResponseState = response.AttemptId, response.Status
			sample.MultipleAttempt = response.AttemptId > 1
		}
		if err != nil || response == nil || response.JobId != jobID || response.AttemptId != 1 ||
			response.Status != "Succeeded" || response.Error != "" || response.Output == "" {
			sample.Failure = "rpc_or_response_validation"
		}
		return sample
	}
	audit := func(sample *submitBenchmarkSample) {
		record, ok, err := f.store.Load(sample.JobID)
		if ok {
			sample.DurableAttempt = record.AttemptID
			sample.DurableState = fmt.Sprintf("JobState(%d)", record.State)
			if record.State == JobSucceeded {
				sample.DurableState = "Succeeded"
			}
		}
		if err != nil || !ok || record != (JobRecord{JobID: sample.JobID, State: JobSucceeded, AttemptID: 1,
			WorkerID: submitBenchWorkerID, TaskType: "zk_preimage_prove", Payload: f.payload, TimeoutMs: submitBenchTaskMs}) {
			sample.Failure = "durable_state_audit"
		}
	}
	warmupSamples := make([]submitBenchmarkSample, 0, submitBenchWarmup)
	for id := int64(1); id <= submitBenchWarmup; id++ {
		sample := run(id)
		audit(&sample)
		identifySubmitBenchmarkSample(&sample, invocation, "warmup", int(id))
		warmupSamples = append(warmupSamples, sample)
		if sample.Failure != "" {
			b.Fatalf("warmup job %d failed: %s (%s)", id, sample.Failure, sample.RPCCode)
		}
	}
	coordinatorBefore := gatherIntegrationMetrics(b, f.coordinatorRegistry)
	workerBefore := gatherIntegrationMetrics(b, f.workers[0].registry)
	callsBefore, busyBefore := f.workers[0].calls.Load(), f.workers[0].busy.Load()
	observationStart := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		sample := run(int64(submitBenchWarmup + 1 + i))
		b.StopTimer()
		audit(&sample)
		identifySubmitBenchmarkSample(&sample, invocation, "measured", i+1)
		samples = append(samples, sample)
		if sample.Failure != "" {
			break // Keep and publish the failed sample; never report a valid cohort.
		}
	}
	observationElapsed := time.Since(observationStart)
	coordinatorAfter := gatherIntegrationMetrics(b, f.coordinatorRegistry)
	workerAfter := gatherIntegrationMetrics(b, f.workers[0].registry)
	failed := 0
	cancelled := 0
	var rpcNanoseconds int64
	for _, sample := range samples {
		rpcNanoseconds += sample.RPCNanoseconds
		if sample.Failure != "" {
			failed++
		}
		if sample.RPCCode == codes.Canceled.String() || sample.RPCCode == codes.DeadlineExceeded.String() || sample.ResponseState == "Cancelled" {
			cancelled++
		}
	}
	// The no-fault baseline must have exactly one published/accepted/verified
	// attempt per job. Counters and histograms exclude all three warmup jobs.
	accountingOK := len(samples) == b.N && failed == 0 && int(f.workers[0].calls.Load()-callsBefore) == b.N && f.workers[0].busy.Load()-busyBefore == 0
	source := map[string]string{"source": "submit"}
	for _, name := range []string{"dzkr_jobs_submitted_total", "dzkr_attempts_started_total"} {
		accountingOK = accountingOK && coordinatorAfter.sample(name, source).value-coordinatorBefore.sample(name, source).value == float64(b.N)
	}
	for _, name := range []string{"dzkr_jobs_terminal_total", "dzkr_attempts_finished_total", "dzkr_attempts_rejected_total"} {
		var total float64
		for _, sample := range coordinatorAfter[name] {
			total += sample.value - coordinatorBefore.sample(name, sample.labels).value
		}
		want := float64(b.N)
		if name == "dzkr_attempts_rejected_total" {
			want = 0
		}
		accountingOK = accountingOK && total == want
	}
	for _, metric := range []struct {
		name   string
		labels map[string]string
	}{
		{"dzkr_jobs_terminal_total", map[string]string{"source": "submit", "state": "succeeded"}},
		{"dzkr_attempts_finished_total", map[string]string{"source": "submit", "outcome": "accepted"}},
	} {
		accountingOK = accountingOK && coordinatorAfter.sample(metric.name, metric.labels).value-coordinatorBefore.sample(metric.name, metric.labels).value == float64(b.N)
	}
	for _, name := range []string{"dzkr_job_execution_duration_seconds", "dzkr_zk_verification_duration_seconds"} {
		labels := source
		if name == "dzkr_zk_verification_duration_seconds" {
			labels = nil
		}
		accountingOK = accountingOK && coordinatorAfter.sample(name, labels).count-coordinatorBefore.sample(name, labels).count == uint64(b.N)
	}
	accepted := map[string]string{"source": "submit", "outcome": "accepted"}
	accountingOK = accountingOK && coordinatorAfter.sample("dzkr_attempt_duration_seconds", accepted).count-coordinatorBefore.sample("dzkr_attempt_duration_seconds", accepted).count == uint64(b.N)
	accountingOK = accountingOK && workerAfter.sample("dzkr_zk_proving_duration_seconds", nil).count-workerBefore.sample("dzkr_zk_proving_duration_seconds", nil).count == uint64(b.N)
	report := struct {
		RunID              string                  `json:"run_id"`
		Invocation         string                  `json:"invocation"`
		Iterations         int                     `json:"iterations"`
		Warmup             int                     `json:"warmup_jobs"`
		Succeeded          int                     `json:"succeeded"`
		Failed             int                     `json:"failed"`
		Cancelled          int                     `json:"cancelled_subset"`
		AccountingOK       bool                    `json:"accounting_ok"`
		RPCSumNS           int64                   `json:"rpc_sum_ns"`
		ObservationNS      int64                   `json:"observation_ns_including_audits"`
		NumCPU             int                     `json:"num_cpu"`
		GOMAXPROCS         int                     `json:"gomaxprocs"`
		SQLite             map[string]any          `json:"sqlite"`
		Configuration      map[string]any          `json:"configuration"`
		WorkerCalls        int32                   `json:"worker_calls"`
		Busy               int32                   `json:"busy"`
		WarmupSamples      []submitBenchmarkSample `json:"warmup_samples"`
		Samples            []submitBenchmarkSample `json:"samples"`
		CoordinatorMetrics []submitBenchmarkMetric `json:"coordinator_metrics_delta"`
		WorkerMetrics      []submitBenchmarkMetric `json:"worker_metrics_delta"`
	}{
		RunID: submitBenchmarkRunID, Invocation: invocation, Iterations: b.N, Warmup: submitBenchWarmup,
		Succeeded: len(samples) - failed, Failed: failed, Cancelled: cancelled, AccountingOK: accountingOK, RPCSumNS: rpcNanoseconds, ObservationNS: observationElapsed.Nanoseconds(),
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), SQLite: f.sqlite,
		Configuration: map[string]any{"workers": 1, "clients": 1, "proving_capacity": 1, "worker_timeout_ms": submitBenchTaskMs,
			"lease_seconds": submitBenchLease.Seconds(), "rpc_deadline_seconds": submitBenchRPCBudget.Seconds(),
			"startup_readiness_seconds": submitBenchSetup.Seconds(), "cleanup_seconds_per_stage": submitBenchCleanup.Seconds(),
			"worker_graceful_cleanup_seconds": (submitBenchCleanup / 2).Seconds(),
			"metrics":                         "separate production Prometheus registries; no HTTP exporter", "client_connection": "warm",
			"dispatch_connection": "new per attempt", "heartbeat_and_recovery_loops": false, "production_worker_service": false},
		WorkerCalls: f.workers[0].calls.Load() - callsBefore, Busy: f.workers[0].busy.Load() - busyBefore, WarmupSamples: warmupSamples, Samples: samples,
		CoordinatorMetrics: submitBenchmarkMetrics(coordinatorBefore, coordinatorAfter), WorkerMetrics: submitBenchmarkMetrics(workerBefore, workerAfter),
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Printf("P3B1_RESULT %s\n", encoded) // No payloads, witnesses, proofs or raw errors.
	if !accountingOK {
		b.Fatal("P3-B1 outcome/accounting mismatch; inspect the retained P3B1_RESULT cohort")
	}
}
