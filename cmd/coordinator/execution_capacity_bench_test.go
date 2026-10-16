package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/status"
)

const capacityWarmup = 6
const capacityCohortBudget = 120 * time.Second

type capacitySlot struct {
	issued  atomic.Bool
	startNS atomic.Int64
}
type capacitySample struct {
	submitBenchmarkSample
	Config            string `json:"config"`
	Client            int    `json:"client_index"`
	Issued            bool   `json:"issued"`
	StartNS           int64  `json:"start_ns"`
	EndNS             int64  `json:"end_ns"`
	Censored          bool   `json:"censored"`
	ResponseJobID     int64  `json:"response_job_id"`
	ResponseHasOutput bool   `json:"response_has_output"`
	ResponseHasError  bool   `json:"response_has_error"`
	DurableWorker     string `json:"durable_worker"`
	SpecMatches       bool   `json:"durable_spec_matches"`
}

// The same Ready ClientConn is shared by bounded logical clients. Only actual
// SubmitJob is inside rpc_ns; atomics for partial-run diagnostics precede it.
func runCapacityRequest(f *submitBenchmarkFixture, invocation, config, cohort string, client, iteration int, jobID int64, origin time.Time, slots ...*capacitySlot) capacitySample {
	request := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "zk_preimage_prove", Payload: f.payload, TimeoutMs: submitBenchTaskMs}
	ctx, cancel := context.WithTimeout(f.ctx, submitBenchRPCBudget)
	defer cancel()
	if len(slots) > 0 {
		slots[0].startNS.Store(time.Since(origin).Nanoseconds())
		slots[0].issued.Store(true)
	}
	start := time.Now()
	response, err := f.client.SubmitJob(ctx, request)
	end := time.Now()
	sample := capacitySample{Config: config, Client: client, Issued: true, StartNS: start.Sub(origin).Nanoseconds(), EndNS: end.Sub(origin).Nanoseconds()}
	sample.JobID = jobID
	sample.RPCNanoseconds = end.Sub(start).Nanoseconds()
	sample.RPCCode = status.Code(err).String()
	identifySubmitBenchmarkSample(&sample.submitBenchmarkSample, invocation, cohort, iteration)
	if response != nil {
		sample.AttemptID = response.AttemptId
		sample.ResponseJobID = response.JobId
		sample.ResponseState = response.Status
		sample.MultipleAttempt = response.AttemptId > 1
		sample.ResponseHasOutput = response.Output != ""
		sample.ResponseHasError = response.Error != ""
	}
	return sample
}
func capacityState(state JobState) string {
	switch state {
	case JobQueued:
		return "Queued"
	case JobRunning:
		return "Running"
	case JobSucceeded:
		return "Succeeded"
	case JobFailed:
		return "Failed"
	case JobCancelled:
		return "Cancelled"
	case JobRecovering:
		return "Recovering"
	default:
		return "Unknown"
	}
}

type capacityDurable struct {
	jobID         int64
	state, worker string
	attempt       int64
	specMatches   bool
}

func auditCapacityJobs(f *submitBenchmarkFixture, samples []capacitySample) ([]capacityDurable, error) {
	rows := make([]capacityDurable, 0, len(samples))
	for _, sample := range samples {
		record, ok, err := f.store.Load(sample.JobID)
		if err != nil {
			return rows, fmt.Errorf("SQLite audit failed for job %d", sample.JobID)
		}
		row := capacityDurable{jobID: sample.JobID, state: "Missing"}
		if ok {
			row.state, row.worker, row.attempt = capacityState(record.State), record.WorkerID, record.AttemptID
			workerKnown := false
			for _, w := range f.workers {
				workerKnown = workerKnown || w.id == record.WorkerID
			}
			row.specMatches = record.JobID == sample.JobID && record.TaskType == "zk_preimage_prove" && record.Payload == f.payload && record.TimeoutMs == submitBenchTaskMs && workerKnown
		}
		rows = append(rows, row)
	}
	return rows, nil
}
func applyCapacityAudit(samples []capacitySample, rows []capacityDurable) {
	byID := make(map[int64]capacityDurable, len(rows))
	for _, r := range rows {
		byID[r.jobID] = r
	}
	for i := range samples {
		r, ok := byID[samples[i].JobID]
		if !ok {
			r.state = "Missing"
		}
		samples[i].DurableState, samples[i].DurableAttempt, samples[i].DurableWorker, samples[i].SpecMatches = r.state, r.attempt, r.worker, r.specMatches
	}
}

type capacityWorkerReport struct {
	Capacity     int                     `json:"capacity"`
	IntervalPeak int                     `json:"prove_interval_peak"`
	WorkerID     string                  `json:"worker_id"`
	Calls        int32                   `json:"calls"`
	Busy         int32                   `json:"busy"`
	Proofs       capacityProofSnapshot   `json:"proofs"`
	Dropped      int64                   `json:"dropped_events"`
	Events       []capacityEvent         `json:"events"`
	Metrics      []submitBenchmarkMetric `json:"metrics_delta"`
}
type capacityReport struct {
	ExpectedOutcomes   bool                    `json:"expected_capacity_outcomes"`
	Rejected           float64                 `json:"rejected_attempts"`
	IntervalPeak       int                     `json:"prove_interval_peak"`
	RunID              string                  `json:"run_id"`
	Invocation         string                  `json:"invocation"`
	Config             string                  `json:"config"`
	Iterations         int                     `json:"iterations"`
	Issued             int                     `json:"issued"`
	Warmup             []capacitySample        `json:"warmup_samples"`
	Samples            []capacitySample        `json:"samples"`
	WarmupCalls        map[string]int32        `json:"warmup_worker_calls"`
	RPCStatus          map[string]int          `json:"rpc_status_counts"`
	Durable            map[string]int          `json:"durable_counts"`
	Succeeded          int                     `json:"confirmed_successes"`
	Attempts           float64                 `json:"attempts"`
	Calls              int32                   `json:"worker_calls"`
	Busy               int32                   `json:"busy"`
	Proofs             capacityProofSnapshot   `json:"proofs"`
	ObservationNS      int64                   `json:"observation_ns"`
	CohortStartNS      int64                   `json:"cohort_start_ns_from_fixture"`
	Throughput         *float64                `json:"successful_jobs_per_second"`
	Amplification      *float64                `json:"attempt_amplification"`
	BusyRate           *float64                `json:"busy_rate"`
	AccountingOK       bool                    `json:"accounting_ok"`
	Valid              bool                    `json:"valid"`
	Comparable         bool                    `json:"comparable"`
	Watchdog           bool                    `json:"watchdog_fired"`
	ClientsJoined      bool                    `json:"clients_joined"`
	Cleanup            capacityCleanup         `json:"cleanup"`
	Configuration      map[string]any          `json:"configuration"`
	KeyHashes          map[string]string       `json:"key_sha256"`
	SQLite             map[string]any          `json:"sqlite"`
	CoordinatorMetrics []submitBenchmarkMetric `json:"coordinator_metrics_delta"`
	Workers            []capacityWorkerReport  `json:"workers"`
}

func capacityRatio(numerator, denominator float64) *float64 {
	if denominator <= 0 {
		return nil
	}
	value := numerator / denominator
	return &value
}

// Each selected child owns its fixture. Example exact selection:
// -bench '^BenchmarkSubmitJobCapacity$/^C$'. DZKR_BENCH_PREIMAGE_DIR is an explicit
// test-only opt-in, containing one externally prepared matching key pair.
// Go ns/op covers the entire coordinated cohort and joins, divided by b.N; it
// is not RPC latency. Audits, Gather, cleanup and JSON/CSV are outside timing.
func BenchmarkSubmitJobCapacity(b *testing.B) {
	for _, cfg := range []struct {
		id                         string
		workers, capacity, clients int
	}{{"A", 1, 1, 1}, {"B", 1, 1, 2}, {"C", 1, 2, 2}, {"D", 2, 1, 2}} {
		b.Run(cfg.id, func(b *testing.B) {
			if b.N > 1000 {
				b.Fatal("bounded capacity benchmark: maximum 1000 jobs")
			}
			dir := os.Getenv("DZKR_BENCH_PREIMAGE_DIR")
			if dir == "" {
				b.Fatal("DZKR_BENCH_PREIMAGE_DIR must name a temporary common setup directory")
			}
			f := newConfiguredSubmitBenchmarkFixture(b, submitBenchmarkConfig{workers: cfg.workers, capacity: cfg.capacity, keyDir: dir, track: true})
			invocation := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
			report := capacityReport{RunID: submitBenchmarkRunID, Invocation: invocation, Config: cfg.id, Iterations: b.N,
				WarmupCalls: make(map[string]int32), RPCStatus: make(map[string]int), Durable: make(map[string]int), KeyHashes: f.keyHashes, SQLite: f.sqlite}
			for id := int64(1); id <= capacityWarmup; id++ {
				sample := runCapacityRequest(f, invocation, cfg.id, "warmup", 0, int(id), id, f.origin)
				audited, err := auditCapacityJobs(f, []capacitySample{sample})
				one := []capacitySample{sample}
				applyCapacityAudit(one, audited)
				sample = one[0]
				report.Warmup = append(report.Warmup, sample)
				if err != nil || sample.RPCCode != "OK" || sample.ResponseState != "Succeeded" || sample.DurableState != "Succeeded" || !sample.SpecMatches || sample.AttemptID != 1 {
					b.Fatal("capacity warmup failed; inspect RPC/SQLite diagnostics")
				}
			}
			coBefore := gatherIntegrationMetrics(b, f.coordinatorRegistry)
			workerBefore := make([]integrationMetricsView, len(f.workers))
			callBefore, busyBefore := make([]int32, len(f.workers)), make([]int32, len(f.workers))
			for i, w := range f.workers {
				workerBefore[i] = gatherIntegrationMetrics(b, w.registry)
				callBefore[i], busyBefore[i] = w.calls.Load(), w.busy.Load()
				report.WarmupCalls[w.id] = callBefore[i]
				if cfg.workers == 2 && callBefore[i] < 3 {
					b.Fatal("each Worker must handle three successful warmup jobs")
				}
				w.drainEvents()
				w.proofs.reset()
			}
			f.proofs.reset()
			slots := make([]capacitySlot, b.N)
			results := make(chan capacitySample, b.N)
			ready, done := make(chan struct{}, cfg.clients), make(chan struct{}, cfg.clients)
			barrier := make(chan struct{})
			var cohortStart time.Time
			for client := 0; client < cfg.clients; client++ {
				go func() {
					defer func() { done <- struct{}{} }()
					ready <- struct{}{}
					select {
					case <-barrier:
					case <-f.ctx.Done():
						return
					}
					for index := client; index < b.N; index += cfg.clients {
						if f.ctx.Err() != nil {
							return
						}
						row := runCapacityRequest(f, invocation, cfg.id, "measured", client, index+1, int64(capacityWarmup+index+1), cohortStart, &slots[index])
						results <- row // Sized for every possible issued job; never waits on collector.
					}
				}()
			}
			readyCtx, cancelReady := context.WithTimeout(f.ctx, submitBenchSetup)
			for range cfg.clients {
				select {
				case <-ready:
				case <-readyCtx.Done():
					cancelReady()
					b.Fatal("closed-loop clients not ready")
				}
			}
			cancelReady()
			b.ReportAllocs()
			b.ResetTimer()
			cohortStart = time.Now()
			report.CohortStartNS = cohortStart.Sub(f.origin).Nanoseconds()
			watchdog := time.NewTimer(capacityCohortBudget)
			close(barrier)
			joined := 0
		waitClients:
			for joined < cfg.clients {
				select {
				case <-done:
					joined++
				case <-watchdog.C:
					report.Watchdog = true
					report.ObservationNS = time.Since(cohortStart).Nanoseconds()
					f.cancel()
					break waitClients
				}
			}
			watchdog.Stop()
			if joined < cfg.clients {
				timer := time.NewTimer(submitBenchSetup)
			drainClients:
				for joined < cfg.clients {
					select {
					case <-done:
						joined++
					case <-timer.C:
						break drainClients
					}
				}
				timer.Stop()
			}
			b.StopTimer()
			report.ClientsJoined = joined == cfg.clients
			collected := make(map[int64]capacitySample, b.N)
		drainResults:
			for {
				select {
				case row := <-results:
					collected[row.JobID] = row
				default:
					break drainResults
				}
			}
			for i := range slots {
				if !slots[i].issued.Load() {
					continue
				}
				id := int64(capacityWarmup + i + 1)
				row, ok := collected[id]
				if !ok {
					row = capacitySample{Config: cfg.id, Client: i % cfg.clients, Issued: true, Censored: true, StartNS: slots[i].startNS.Load()}
					row.JobID = id
					row.RPCCode = "Unresolved"
					identifySubmitBenchmarkSample(&row.submitBenchmarkSample, invocation, "measured", i+1)
				}
				report.Samples = append(report.Samples, row)
				if !report.Watchdog && row.EndNS > report.ObservationNS {
					report.ObservationNS = row.EndNS
				}
			}
			report.Issued = len(report.Samples)
			auditResult := make(chan []capacityDurable, 1)
			auditSamples := append([]capacitySample(nil), report.Samples...)
			// P1 invokes this only after admitted handlers and dispatch have joined,
			// immediately before SQLite Close. The buffered handoff is safe on timeout.
			f.beforeClose = func() error { rows, err := auditCapacityJobs(f, auditSamples); auditResult <- rows; return err }
			cleanupErr := f.finish()
			report.Cleanup = f.cleanup
			select {
			case rows := <-auditResult:
				applyCapacityAudit(report.Samples, rows)
			default:
			}
			coAfter := gatherIntegrationMetrics(b, f.coordinatorRegistry)
			report.CoordinatorMetrics = submitBenchmarkMetrics(coBefore, coAfter)
			report.Attempts = coAfter.sample("dzkr_attempts_started_total", map[string]string{"source": "submit"}).value - coBefore.sample("dzkr_attempts_started_total", map[string]string{"source": "submit"}).value
			for i, w := range f.workers {
				after := gatherIntegrationMetrics(b, w.registry)
				wr := capacityWorkerReport{WorkerID: w.id, Capacity: cfg.capacity, Calls: w.calls.Load() - callBefore[i], Busy: w.busy.Load() - busyBefore[i], Proofs: w.proofs.snapshot(), Dropped: w.dropped.Load(), Events: w.drainEvents(), Metrics: submitBenchmarkMetrics(workerBefore[i], after)}
				report.Workers = append(report.Workers, wr)
				report.Calls += wr.Calls
				report.Busy += wr.Busy
			}
			report.Proofs = f.proofs.snapshot()
			report.Configuration = map[string]any{"workers": cfg.workers, "capacity_per_worker": cfg.capacity, "clients": cfg.clients, "client_connection": "one shared Ready connection", "dispatch_connection": "new per attempt", "warmup": capacityWarmup, "timeout_ms": submitBenchTaskMs, "lease_seconds": submitBenchLease.Seconds(), "rpc_seconds": submitBenchRPCBudget.Seconds(), "cohort_watchdog_seconds": capacityCohortBudget.Seconds(), "worker_cleanup_shared_seconds": submitBenchCleanup.Seconds(), "coordinator_cleanup_seconds": submitBenchCleanup.Seconds(), "readiness_seconds": submitBenchSetup.Seconds(), "constraint_count": 331, "gomaxprocs": runtime.GOMAXPROCS(0), "num_cpu": runtime.NumCPU(), "go_version": runtime.Version(), "metrics": "separate Prometheus registries without HTTP exporter", "worker_boundary": "test endpoint; production Task/Runtime and rpcauth; no WorkerService/lifecycle/heartbeat", "recovery_liveness_loops": false, "resource_boundary": "all endpoints, Coordinator and clients share one Go process CPU/GC budget"}
			payloadHash := sha256.Sum256([]byte(f.payload))
			report.Configuration["public_payload_sha256"] = fmt.Sprintf("%x", payloadHash)
			finalizeCapacityAccounting(&report, coBefore, coAfter)
			report.Valid = report.AccountingOK && cleanupErr == nil && report.ClientsJoined && !report.Watchdog
			report.Comparable = report.Valid && report.ExpectedOutcomes && report.Proofs.AfterCancel == 0 && report.Rejected == 0
			report.Throughput = capacityRatio(float64(report.Succeeded), float64(report.ObservationNS)/1e9)
			report.Amplification = capacityRatio(report.Attempts, float64(report.Issued))
			report.BusyRate = capacityRatio(float64(report.Busy), float64(report.Calls))
			encoded, err := json.Marshal(report)
			if err != nil {
				b.Fatal(err)
			}
			fmt.Printf("P3B2_RESULT %s\n", encoded)
			if !report.Valid {
				b.Fatal("invalid capacity cohort; retained P3B2_RESULT includes all observed outcomes and cleanup status")
			}
		})
	}
}
func finalizeCapacityAccounting(r *capacityReport, before, after integrationMetricsView) {
	var intervals []capacityEvent
	for i := range r.Workers {
		r.Workers[i].IntervalPeak = capacityIntervalPeak(r.Workers[i].Events)
		intervals = append(intervals, r.Workers[i].Events...)
	}
	r.IntervalPeak = capacityIntervalPeak(intervals)
	r.ExpectedOutcomes = true
	ok := r.Issued == r.Iterations && len(r.Samples) == r.Issued
	busyByJob := make(map[int64]int)
	proveByJob := make(map[int64]int)
	var calls, busy, proofs int64
	for _, w := range r.Workers {
		ok = ok && w.Dropped == 0 && w.Proofs.Active == 0 && w.Proofs.Completed == w.Proofs.Started
		var eventCalls, eventBusy, eventProves int64
		for _, e := range w.Events {
			if e.Kind == "rpc" {
				eventCalls++
				if e.Code == "ResourceExhausted" {
					eventBusy++
					busyByJob[e.JobID]++
				}
			}
			if e.Kind == "prove" {
				eventProves++
				proveByJob[e.JobID]++
				ok = ok && e.EndNS >= e.StartNS
				r.ExpectedOutcomes = r.ExpectedOutcomes && e.Proved && !e.Cancelled
			}
		}
		var histogramCount uint64
		for _, m := range w.Metrics {
			if m.Name == "dzkr_zk_proving_duration_seconds" {
				histogramCount += m.Count
			}
		}
		ok = ok && eventCalls == int64(w.Calls) && eventBusy == int64(w.Busy) && eventProves == w.Proofs.Completed && histogramCount == uint64(eventProves)
		calls += eventCalls
		busy += eventBusy
		proofs += eventProves
	}
	for _, s := range r.Samples {
		r.RPCStatus[s.RPCCode]++
		r.Durable[s.DurableState]++
		ok = ok && s.SpecMatches && !s.Censored && s.DurableAttempt >= 1 && s.DurableAttempt <= 2
		switch s.DurableState {
		case "Succeeded":
			matched := s.RPCCode == "OK" && s.ResponseState == "Succeeded" && s.ResponseJobID == s.JobID && s.AttemptID == s.DurableAttempt && s.ResponseHasOutput && !s.ResponseHasError
			if matched {
				r.Succeeded++
			}
			ok = ok && matched && proveByJob[s.JobID] >= 1
		case "Failed":
			// Expected overload exhaustion is an RPC error plus durable Failed, not
			// a harness failure. Do not infer this only from the top-level RPC code.
			expectedBusy := s.RPCCode == "Unavailable" && busyByJob[s.JobID] == int(s.DurableAttempt) && proveByJob[s.JobID] == 0
			taskFailed := s.RPCCode == "OK" && strings.EqualFold(s.ResponseState, "Failed") && s.AttemptID == s.DurableAttempt
			ok = ok && (s.RPCCode == "Unavailable" || taskFailed)
			r.ExpectedOutcomes = r.ExpectedOutcomes && expectedBusy
		case "Cancelled":
			r.ExpectedOutcomes = false
			ok = ok && s.RPCCode == "OK" && strings.EqualFold(s.ResponseState, "Cancelled") && s.AttemptID == s.DurableAttempt
		default:
			ok = false
		}
	}
	metricDelta := func(name string, labels map[string]string) float64 {
		return after.sample(name, labels).value - before.sample(name, labels).value
	}
	source := map[string]string{"source": "submit"}
	ok = ok && metricDelta("dzkr_jobs_submitted_total", source) == float64(r.Issued) && r.Attempts >= float64(r.Issued) && r.Attempts <= float64(2*r.Issued)
	var finished, rejected float64
	for _, s := range after["dzkr_attempts_finished_total"] {
		finished += s.value - before.sample("dzkr_attempts_finished_total", s.labels).value
	}
	for _, s := range after["dzkr_attempts_rejected_total"] {
		rejected += s.value - before.sample("dzkr_attempts_rejected_total", s.labels).value
	}
	r.Rejected = rejected
	ok = ok && finished == r.Attempts
	for _, state := range []string{"Succeeded", "Failed", "Cancelled"} {
		labels := map[string]string{"source": "submit", "state": strings.ToLower(state)}
		ok = ok && metricDelta("dzkr_jobs_terminal_total", labels) == float64(r.Durable[state])
	}
	verified := after.sample("dzkr_zk_verification_duration_seconds", nil).count - before.sample("dzkr_zk_verification_duration_seconds", nil).count
	ok = ok && verified >= uint64(r.Durable["Succeeded"]) && r.Proofs.Started == r.Proofs.Completed && r.Proofs.Active == 0 && proofs == r.Proofs.Completed && calls == int64(r.Calls) && busy == int64(r.Busy)
	for _, w := range r.Workers {
		if w.Capacity > 0 {
			ok = ok && w.IntervalPeak <= w.Capacity
		}
	}
	r.AccountingOK = ok
	sort.Slice(r.Samples, func(i, j int) bool { return r.Samples[i].Iteration < r.Samples[j].Iteration })
}

// Half-open intervals exclude tracker atomics before/after the delegated call.
// They establish overlapping call lifetimes, not simultaneous CPU execution.
func capacityIntervalPeak(events []capacityEvent) int {
	type point struct {
		ns    int64
		delta int
	}
	var points []point
	for _, e := range events {
		if e.Kind == "prove" {
			points = append(points, point{e.StartNS, 1}, point{e.EndNS, -1})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].ns == points[j].ns {
			return points[i].delta < points[j].delta
		}
		return points[i].ns < points[j].ns
	})
	active, peak := 0, 0
	for _, p := range points {
		active += p.delta
		if active > peak {
			peak = active
		}
	}
	return peak
}
