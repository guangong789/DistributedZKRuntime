package main

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type integrationMetricsEndpoint struct {
	registry *prom.Registry
	backend  *prommetrics.Metrics
	url      string
}

func newIntegrationMetricsEndpoint(t *testing.T) integrationMetricsEndpoint {
	t.Helper()
	registry := prom.NewRegistry()
	backend, err := prommetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	server, err := prommetrics.StartHTTP("127.0.0.1:0", registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("metrics shutdown: %v", err)
		}
	})
	return integrationMetricsEndpoint{registry: registry, backend: backend, url: "http://" + server.Addr() + "/metrics"}
}

type integrationMetricSample struct {
	labels map[string]string
	value  float64
	count  uint64
	sum    float64
}

type integrationMetricsView map[string][]integrationMetricSample

func integrationMetricsFromFamilies(families []*dto.MetricFamily) integrationMetricsView {
	view := make(integrationMetricsView)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			sample := integrationMetricSample{labels: make(map[string]string)}
			for _, label := range metric.GetLabel() {
				sample.labels[label.GetName()] = label.GetValue()
			}
			if counter := metric.GetCounter(); counter != nil {
				sample.value = counter.GetValue()
			}
			if histogram := metric.GetHistogram(); histogram != nil {
				sample.count, sample.sum = histogram.GetSampleCount(), histogram.GetSampleSum()
			}
			view[family.GetName()] = append(view[family.GetName()], sample)
		}
	}
	return view
}

func gatherIntegrationMetrics(t *testing.T, registry *prom.Registry) integrationMetricsView {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	return integrationMetricsFromFamilies(families)
}

func scrapeIntegrationMetrics(t *testing.T, url string) integrationMetricsView {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics HTTP status=%d", resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	parsed, err := parser.TextToMetricFamilies(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		t.Fatalf("parse Prometheus exposition: %v", err)
	}
	families := make([]*dto.MetricFamily, 0, len(parsed))
	for _, family := range parsed {
		families = append(families, family)
	}
	return integrationMetricsFromFamilies(families)
}

func (view integrationMetricsView) sample(name string, labels map[string]string) integrationMetricSample {
	for _, sample := range view[name] {
		match := len(sample.labels) == len(labels)
		for key, value := range labels {
			actual, ok := sample.labels[key]
			match = match && ok && actual == value
		}
		if match {
			return sample
		}
	}
	// Vector series are absent before the first event; absence means zero.
	return integrationMetricSample{}
}

func assertIntegrationCounter(t *testing.T, view integrationMetricsView, name string, labels map[string]string, want float64) {
	t.Helper()
	if got := view.sample(name, labels).value; got != want {
		t.Fatalf("%s%v=%v, want %v", name, labels, got, want)
	}
}

func assertIntegrationHistogram(t *testing.T, view integrationMetricsView, name string, labels map[string]string, want uint64) {
	t.Helper()
	sample := view.sample(name, labels)
	if sample.count != want || sample.sum < 0 || math.IsNaN(sample.sum) || math.IsInf(sample.sum, 0) {
		t.Fatalf("%s%v count=%d sum=%v, want count=%d and finite nonnegative sum", name, labels, sample.count, sample.sum, want)
	}
}

func assertIntegrationCounterTotal(t *testing.T, view integrationMetricsView, name, source string, want float64) {
	t.Helper()
	var total float64
	for _, sample := range view[name] {
		if source == "" || sample.labels["source"] == source {
			total += sample.value
		}
	}
	if total != want {
		t.Fatalf("%s total for source %q=%v, want %v", name, source, total, want)
	}
}

type integrationZKFixture struct {
	prover    *zk.PreimageProver
	verifier  *zk.PreimageVerifier
	witnesses runtime.WitnessStore
	payload   string
}

func newIntegrationZKFixture(t *testing.T) integrationZKFixture {
	t.Helper()
	dir := t.TempDir()
	pk, vk := filepath.Join(dir, "proving.key"), filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(pk, vk); err != nil {
		t.Fatal(err)
	}
	prover, err := zk.LoadPreimageProver(pk)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := zk.LoadPreimageVerifier(vk)
	if err != nil {
		t.Fatal(err)
	}
	witnesses := runtime.NewMemoryWitnessStore()
	witnesses.Put("secret-001", 42)
	return integrationZKFixture{
		prover: prover, verifier: verifier, witnesses: witnesses,
		payload: preimagePublicPayload(zk.ComputePreimageDigest(42)),
	}
}

func (fixture integrationZKFixture) execute(ctx context.Context, req *runtimepb.ExecuteJobRequest, observer metrics.Metrics) (*runtimepb.ExecuteJobResponse, error) {
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
	defer cancel()
	task := runtime.ZKPreimageTask{
		Payload: req.Payload, Prover: fixture.prover, WitnessStore: fixture.witnesses, Metrics: observer,
	}
	result := runtime.ExecuteJob(jobCtx, runtime.Job{ID: int(req.JobId), Task: task, Status: runtime.JobRunning})
	errorText := ""
	if result.Err != nil {
		errorText = result.Err.Error()
	}
	return &runtimepb.ExecuteJobResponse{
		JobId: int64(result.JobID), AttemptId: req.AttemptId,
		Status: result.Status.String(), Output: result.Output, Error: errorText,
	}, nil
}

func TestPrometheusMetricsZKExecution(t *testing.T) {
	for _, tc := range []struct {
		name            string
		failedOutcome   metrics.AttemptOutcome
		rejectReason    metrics.AttemptRejectReason
		verifyCount     uint64
		firstProveCount uint64
	}{
		{name: "success", verifyCount: 1, firstProveCount: 1},
		{name: "rpc retry", failedOutcome: metrics.AttemptRPCError, verifyCount: 1},
		{name: "fencing", failedOutcome: metrics.AttemptFenced, rejectReason: metrics.RejectFencing, verifyCount: 1, firstProveCount: 1},
		{name: "invalid proof", failedOutcome: metrics.AttemptProofRejected, rejectReason: metrics.RejectProof, verifyCount: 2, firstProveCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const jobID int64 = 9201
			fixture := newIntegrationZKFixture(t)
			store := newWorkerFailureStore(t)
			coordinatorMetrics := newIntegrationMetricsEndpoint(t)
			firstMetrics := newIntegrationMetricsEndpoint(t)
			s := &CoordinatorServer{
				jobStore: store, preimageVerifier: fixture.verifier, metrics: coordinatorMetrics.backend,
			}
			client := startTestCoordinator(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			firstStarted, secondStarted := make(chan int64, 1), make(chan int64, 1)
			failFirst, releaseSecond := make(chan struct{}), make(chan struct{})
			var firstRelease, secondRelease sync.Once
			defer firstRelease.Do(func() { close(failFirst) })
			defer secondRelease.Do(func() { close(releaseSecond) })
			var firstCalls, secondCalls atomic.Int32
			startWorkerFailureServer(t, client, "worker-1", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				firstCalls.Add(1)
				firstStarted <- req.AttemptId
				if tc.name == "rpc retry" {
					select {
					case <-failFirst:
						return nil, status.Error(codes.Unavailable, "injected in-flight worker failure")
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				resp, err := fixture.execute(ctx, req, firstMetrics.backend)
				if tc.name == "fencing" {
					resp.AttemptId++
				}
				if tc.name == "invalid proof" {
					resp.Output = base64.StdEncoding.EncodeToString([]byte("corrupted proof"))
				}
				return resp, err
			})
			var secondMetrics integrationMetricsEndpoint
			if tc.name != "success" {
				secondMetrics = newIntegrationMetricsEndpoint(t)
				startWorkerFailureServer(t, client, "worker-2", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					secondCalls.Add(1)
					secondStarted <- req.AttemptId
					select {
					case <-releaseSecond:
						return fixture.execute(ctx, req, secondMetrics.backend)
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				})
			}
			submitted := make(chan workerFailureResult, 1)
			go func() {
				resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
					JobId: jobID, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 5000,
				})
				submitted <- workerFailureResult{resp: resp, err: err}
			}()
			select {
			case attempt := <-firstStarted:
				if attempt != 1 {
					t.Fatalf("first attempt=%d, want 1", attempt)
				}
			case <-ctx.Done():
				t.Fatal("worker-1 did not start")
			}
			firstRelease.Do(func() { close(failFirst) })
			wantAttempts := 1
			if tc.name != "success" {
				wantAttempts = 2
				select {
				case attempt := <-secondStarted:
					if attempt != 2 {
						t.Fatalf("second attempt=%d, want 2", attempt)
					}
				case <-ctx.Done():
					t.Fatal("retry did not reach worker-2")
				}
				before := gatherIntegrationMetrics(t, coordinatorMetrics.registry)
				assertIntegrationCounter(t, before, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}, 1)
				assertIntegrationCounter(t, before, "dzkr_attempts_started_total", map[string]string{"source": "submit"}, 2)
				assertIntegrationCounterTotal(t, before, "dzkr_attempts_finished_total", "submit", 1)
				assertIntegrationCounterTotal(t, before, "dzkr_jobs_terminal_total", "", 0)
				assertIntegrationCounter(t, before, "dzkr_attempts_finished_total",
					map[string]string{"source": "submit", "outcome": string(tc.failedOutcome)}, 1)
				if tc.rejectReason != "" {
					assertIntegrationCounter(t, before, "dzkr_attempts_rejected_total",
						map[string]string{"source": "submit", "reason": string(tc.rejectReason)}, 1)
				}
				assertIntegrationHistogram(t, before, "dzkr_zk_verification_duration_seconds", nil, tc.verifyCount-1)
				for _, record := range store.savedRecords() {
					if record.State != JobRunning {
						t.Fatalf("rejected Attempt 1 persisted a terminal state: %+v", record)
					}
				}
				secondRelease.Do(func() { close(releaseSecond) })
			}
			select {
			case result := <-submitted:
				if result.err != nil || result.resp == nil || result.resp.Status != "Succeeded" ||
					result.resp.JobId != jobID || result.resp.AttemptId != int64(wantAttempts) {
					t.Fatalf("ZK response=%+v error=%v", result.resp, result.err)
				}
			case <-ctx.Done():
				t.Fatal("ZK submission did not finish")
			}
			if firstCalls.Load() != 1 || int(secondCalls.Load()) != wantAttempts-1 {
				t.Fatalf("unexpected Worker calls: first=%d second=%d", firstCalls.Load(), secondCalls.Load())
			}
			final := gatherIntegrationMetrics(t, coordinatorMetrics.registry)
			assertIntegrationCounterTotal(t, final, "dzkr_jobs_submitted_total", "", 1)
			assertIntegrationCounter(t, final, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}, 1)
			assertIntegrationCounter(t, final, "dzkr_attempts_started_total", map[string]string{"source": "submit"}, float64(wantAttempts))
			assertIntegrationCounterTotal(t, final, "dzkr_attempts_finished_total", "submit", float64(wantAttempts))
			assertIntegrationCounter(t, final, "dzkr_attempts_finished_total", map[string]string{"source": "submit", "outcome": "accepted"}, 1)
			assertIntegrationCounter(t, final, "dzkr_jobs_terminal_total", map[string]string{"source": "submit", "state": "succeeded"}, 1)
			assertIntegrationCounterTotal(t, final, "dzkr_jobs_terminal_total", "", 1)
			for _, reason := range []metrics.AttemptRejectReason{metrics.RejectFencing, metrics.RejectProof} {
				want := float64(0)
				if tc.rejectReason == reason {
					want = 1
				}
				assertIntegrationCounter(t, final, "dzkr_attempts_rejected_total", map[string]string{"source": "submit", "reason": string(reason)}, want)
			}
			assertIntegrationHistogram(t, final, "dzkr_zk_verification_duration_seconds", nil, tc.verifyCount)
			assertIntegrationHistogram(t, final, "dzkr_job_execution_duration_seconds", map[string]string{"source": "submit"}, 1)
			assertIntegrationHistogram(t, final, "dzkr_attempt_duration_seconds", map[string]string{"source": "submit", "outcome": "accepted"}, 1)
			assertIntegrationHistogram(t, gatherIntegrationMetrics(t, firstMetrics.registry), "dzkr_zk_proving_duration_seconds", nil, tc.firstProveCount)
			// Exercise both the Coordinator and Worker real HTTP exposition paths.
			httpCoordinator := scrapeIntegrationMetrics(t, coordinatorMetrics.url)
			assertIntegrationCounter(t, httpCoordinator, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}, 1)
			assertIntegrationCounter(t, httpCoordinator, "dzkr_jobs_terminal_total", map[string]string{"source": "submit", "state": "succeeded"}, 1)
			assertIntegrationHistogram(t, httpCoordinator, "dzkr_zk_verification_duration_seconds", nil, tc.verifyCount)
			assertIntegrationHistogram(t, scrapeIntegrationMetrics(t, firstMetrics.url), "dzkr_zk_proving_duration_seconds", nil, tc.firstProveCount)
			if wantAttempts == 2 {
				assertIntegrationHistogram(t, gatherIntegrationMetrics(t, secondMetrics.registry), "dzkr_zk_proving_duration_seconds", nil, 1)
				assertIntegrationHistogram(t, scrapeIntegrationMetrics(t, secondMetrics.url), "dzkr_zk_proving_duration_seconds", nil, 1)
			}
			writes := store.savedRecords()
			if len(writes) != wantAttempts+1 {
				t.Fatalf("durable writes=%+v", writes)
			}
			for i := 0; i < wantAttempts; i++ {
				if writes[i].State != JobRunning || writes[i].AttemptID != int64(i+1) {
					t.Fatalf("invalid Running progression: %+v", writes)
				}
			}
			wantWorker := "worker-1"
			if wantAttempts == 2 {
				wantWorker = "worker-2"
			}
			assertRecoveryRecord(t, store, JobRecord{
				JobID: jobID, State: JobSucceeded, AttemptID: int64(wantAttempts), WorkerID: wantWorker,
				TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 5000,
			})
		})
	}
}

func TestPrometheusMetricsWorkerTimeoutTransition(t *testing.T) {
	endpoint := newIntegrationMetricsEndpoint(t)
	s := &CoordinatorServer{metrics: endpoint.backend}
	client := startTestCoordinator(t, s)
	startWorkerFailureServer(t, client, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(req), nil
	})
	s.mu.Lock()
	worker := s.workers["worker"]
	worker.LastSeen = time.Now().Add(-time.Hour)
	s.workers[worker.ID] = worker
	s.mu.Unlock()
	s.checkWorkerLiveness(time.Second)
	labels := map[string]string{"from": "alive", "to": "dead", "cause": "heartbeat_timeout"}
	assertIntegrationCounter(t, gatherIntegrationMetrics(t, endpoint.registry), "dzkr_worker_state_transitions_total", labels, 1)
	for i := 0; i < 3; i++ {
		s.checkWorkerLiveness(time.Second)
	}
	assertIntegrationCounter(t, scrapeIntegrationMetrics(t, endpoint.url), "dzkr_worker_state_transitions_total", labels, 1)
	if _, alive := s.getNextWorker(); alive {
		t.Fatal("timed-out Worker remained schedulable")
	}
}

type integrationRecoveryPassMetrics struct {
	metrics.Metrics
	done chan metrics.RecoveryPassOutcome
}

func (m *integrationRecoveryPassMetrics) RecoveryPass(outcome metrics.RecoveryPassOutcome, duration time.Duration) {
	m.Metrics.RecoveryPass(outcome, duration)
	// Signal after the pass event is recorded, avoiding a cancellation race
	// between a terminal Save notification and pass completion.
	select {
	case m.done <- outcome:
	default:
	}
}

func TestPrometheusMetricsZKRecovery(t *testing.T) {
	const jobID int64 = 9202
	fixture := newIntegrationZKFixture(t)
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	dbA, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	coordinatorA := &CoordinatorServer{jobStore: dbA}
	attempt, err := coordinatorA.startAttempt(jobID, "old-worker", time.Minute, "zk_preimage_prove", fixture.payload, 5000)
	if err != nil || attempt != 1 {
		t.Fatalf("persist old attempt=%d error=%v", attempt, err)
	}
	running1 := JobRecord{
		JobID: jobID, State: JobRunning, AttemptID: 1, WorkerID: "old-worker",
		TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 5000,
	}
	assertRecoveryRecord(t, dbA, running1)
	if err := dbA.Close(); err != nil {
		t.Fatal(err)
	}
	dbB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB.Close() })
	store := &workerFailureStore{JobStore: dbB}
	coordinatorMetrics := newIntegrationMetricsEndpoint(t)
	workerMetrics := newIntegrationMetricsEndpoint(t)
	passDone := make(chan metrics.RecoveryPassOutcome, 1)
	s := &CoordinatorServer{
		jobStore: store, preimageVerifier: fixture.verifier,
		metrics:       &integrationRecoveryPassMetrics{Metrics: coordinatorMetrics.backend, done: passDone},
		recoveryReady: make(chan struct{}, 1),
	}
	if err := s.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	recovering1 := running1
	recovering1.State = JobRecovering
	assertRecoveryRecord(t, store, recovering1)
	assertPublicRecoveryPayload(t, recovering1.Payload)
	loopCtx, cancelLoop := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() { s.runRecoveryLoop(loopCtx); close(loopDone) }()
	t.Cleanup(func() { cancelLoop(); <-loopDone })
	client := startTestCoordinator(t, s)
	var calls atomic.Int32
	startWorkerFailureServer(t, client, "recovered-worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.AttemptId != 2 || req.TaskType != "zk_preimage_prove" ||
			req.Payload != fixture.payload || req.TimeoutMs != 5000 {
			t.Errorf("recovery dispatch=%+v", req)
		}
		return fixture.execute(ctx, req, workerMetrics.backend)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case outcome := <-passDone:
		if outcome != metrics.PassCompleted {
			t.Fatalf("recovery pass outcome=%s", outcome)
		}
	case <-ctx.Done():
		t.Fatal("registration did not finish recovery")
	}
	cancelLoop()
	select {
	case <-loopDone:
	case <-ctx.Done():
		t.Fatal("recovery loop did not exit")
	}
	final := gatherIntegrationMetrics(t, coordinatorMetrics.registry)
	assertIntegrationCounter(t, final, "dzkr_recovery_transitions_total", map[string]string{"cause": "startup"}, 1)
	assertIntegrationCounter(t, final, "dzkr_recovery_passes_total", map[string]string{"outcome": "completed"}, 1)
	assertIntegrationCounterTotal(t, final, "dzkr_recovery_passes_total", "", 1)
	assertIntegrationCounterTotal(t, final, "dzkr_jobs_submitted_total", "", 0)
	assertIntegrationCounter(t, final, "dzkr_attempts_started_total", map[string]string{"source": "recovery"}, 1)
	assertIntegrationCounter(t, final, "dzkr_attempts_finished_total", map[string]string{"source": "recovery", "outcome": "accepted"}, 1)
	assertIntegrationCounter(t, final, "dzkr_jobs_terminal_total", map[string]string{"source": "recovery", "state": "succeeded"}, 1)
	assertIntegrationCounterTotal(t, final, "dzkr_recovery_execution_failures_total", "", 0)
	assertIntegrationCounterTotal(t, final, "dzkr_recovery_repair_failures_total", "", 0)
	assertIntegrationHistogram(t, final, "dzkr_zk_verification_duration_seconds", nil, 1)
	assertIntegrationHistogram(t, final, "dzkr_job_execution_duration_seconds", map[string]string{"source": "recovery"}, 1)
	assertIntegrationHistogram(t, final, "dzkr_recovery_pass_duration_seconds", map[string]string{"outcome": "completed"}, 1)
	for name, samples := range final {
		for _, sample := range samples {
			if sample.labels["source"] == "submit" {
				t.Errorf("recovery emitted a submit-source series: %s%v", name, sample.labels)
			}
		}
	}
	httpCoordinator := scrapeIntegrationMetrics(t, coordinatorMetrics.url)
	assertIntegrationCounter(t, httpCoordinator, "dzkr_jobs_terminal_total", map[string]string{"source": "recovery", "state": "succeeded"}, 1)
	assertIntegrationCounter(t, httpCoordinator, "dzkr_recovery_transitions_total", map[string]string{"cause": "startup"}, 1)
	assertIntegrationHistogram(t, scrapeIntegrationMetrics(t, workerMetrics.url), "dzkr_zk_proving_duration_seconds", nil, 1)
	running2 := running1
	running2.AttemptID, running2.WorkerID = 2, "recovered-worker"
	succeeded2 := running2
	succeeded2.State = JobSucceeded
	assertWorkerFailureSaves(t, store, []JobRecord{recovering1, running2, succeeded2})
	assertRecoveryRecord(t, store, succeeded2)
	assertRecoveryOwnershipReleased(t, s, jobID)
	if calls.Load() != 1 {
		t.Fatalf("recovery dispatched %d times, want 1", calls.Load())
	}
}

func TestPrometheusMetricsRecoveryRepair(t *testing.T) {
	for _, mode := range []string{"repair succeeds", "repair Load fails", "repair Save fails"} {
		t.Run(mode, func(t *testing.T) {
			inner := newWorkerFailureStore(t)
			record := JobRecord{
				JobID: 9203, State: JobRecovering, AttemptID: 1, WorkerID: "old-worker",
				TaskType: "zk_preimage_prove", Payload: preimagePublicPayload(zk.ComputePreimageDigest(42)), TimeoutMs: 5000,
			}
			saveRecoveryRecord(t, inner, record)
			coordinatorMetrics := newIntegrationMetricsEndpoint(t)
			var repairLoad atomic.Bool
			store := &recoveryPassStore{
				JobStore: inner,
				load: func(id int64) (JobRecord, bool, error) {
					if repairLoad.Load() {
						return JobRecord{}, false, errors.New("injected repair Load failure")
					}
					return inner.Load(id)
				},
				save: func(got JobRecord) error {
					if mode == "repair Save fails" && got.State == JobRecovering {
						return errors.New("injected repair Save failure")
					}
					return inner.Save(got)
				},
			}
			s := &CoordinatorServer{jobStore: store, metrics: coordinatorMetrics.backend}
			client := startTestCoordinator(t, s)
			var calls atomic.Int32
			startWorkerFailureServer(t, client, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if mode == "repair Load fails" && req.AttemptId == 3 {
					repairLoad.Store(true)
				}
				return nil, status.Error(codes.Unavailable, "injected recovery RPC failure")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.runRecoveryPass(ctx); err != nil {
				t.Fatal(err)
			}
			view := gatherIntegrationMetrics(t, coordinatorMetrics.registry)
			assertIntegrationCounterTotal(t, view, "dzkr_jobs_submitted_total", "", 0)
			assertIntegrationCounter(t, view, "dzkr_attempts_started_total", map[string]string{"source": "recovery"}, 2)
			assertIntegrationCounter(t, view, "dzkr_attempts_finished_total", map[string]string{"source": "recovery", "outcome": "rpc_error"}, 2)
			assertIntegrationCounter(t, view, "dzkr_recovery_execution_failures_total", map[string]string{"reason": "unavailable"}, 1)
			assertIntegrationCounter(t, view, "dzkr_recovery_passes_total", map[string]string{"outcome": "completed_with_errors"}, 1)
			assertIntegrationCounterTotal(t, view, "dzkr_jobs_terminal_total", "", 0)
			assertIntegrationHistogram(t, view, "dzkr_zk_verification_duration_seconds", nil, 0)
			want := record
			want.State, want.AttemptID, want.WorkerID = JobRunning, 3, "worker"
			wantTransition := float64(0)
			if mode == "repair succeeds" {
				want.State, wantTransition = JobRecovering, 1
			}
			assertIntegrationCounter(t, view, "dzkr_recovery_transitions_total", map[string]string{"cause": "execution_failure"}, wantTransition)
			for _, stage := range []string{"load", "save"} {
				wantFailure := float64(0)
				if mode == "repair Load fails" && stage == "load" || mode == "repair Save fails" && stage == "save" {
					wantFailure = 1
				}
				assertIntegrationCounter(t, view, "dzkr_recovery_repair_failures_total", map[string]string{"stage": stage}, wantFailure)
			}
			assertRecoveryRecord(t, inner, want)
			assertRecoveryOwnershipReleased(t, s, record.JobID)
			if calls.Load() != 2 {
				t.Fatalf("RPC calls=%d, want 2", calls.Load())
			}
		})
	}
}
