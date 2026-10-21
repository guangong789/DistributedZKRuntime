# Benchmark and experiment contracts

This is a source-based execution contract, not a claim of new measurements.
[Test selection](testing.md) explains current default behavior and exact gates.
All benchmark roots below require explicit `-bench` selection and are absent from
ordinary default execution; some supporting correctness tests run by default.
Read the resource/fixture preconditions before enabling any experiment.

Selectors below are metadata, **not complete launch commands**. For Go benchmark
commands also select `-run '^$'` so ordinary tests do not become unbudgeted setup.
Use a deliberate fixed iteration count, explicit process/test/wall budgets and
resource monitoring. `-mod=readonly` alone is neither offline mode nor isolation.
Do not use `-bench .` or enable multiple experimental roles together.

## P3-A CPU and execution-layer benchmarks

All use the fixed production BN254 MiMC preimage circuit (331 constraints), real
Groth16 and temporary matching key material. No deployment witnesses/credentials
are needed. Fixture compilation/Setup/key generation are outside timed loops,
but still occur in the process and consume resources. Go calibration can invoke
benchmark fixtures repeatedly; warmup counts below are per invocation.

| Package | Entry / exact `-bench` selector | Warmup / preflight outside timing | Timed operation | Excluded work |
|---|---|---|---|---|
| `./internal/zk` | `BenchmarkPreimageProve` / `^BenchmarkPreimageProve$` | 3 wrapper Prove + Verify pairs | Production wrapper witness construction, Groth16 Prove, binary proof serialization; loop output/error checks | Setup, key loading, digest preparation, last-proof independent Verify |
| `./internal/zk` | `BenchmarkPreimageVerify` / `^BenchmarkPreimageVerify$` | 3 wrapper Prove + Verify pairs | Binary proof decode, public witness construction and real Verify | Setup/key load/fixture proof; no Base64 decode in the timed path |
| `./internal/zk` | `BenchmarkLoadPreimageProver` / `^BenchmarkLoadPreimageProver$` | One prover-load preflight, no fixed 3-proof warmup | Repeated production prover load, including circuit compilation and PK file decode | Setup/key creation; final loaded-prover Prove and independent Verify |
| `./internal/zk` | `BenchmarkLoadPreimageVerifier` / `^BenchmarkLoadPreimageVerifier$` | Prepare one real proof; load verifier and verify once | Repeated VK file load/decode | Setup/proof generation; post-loop Verify |
| `./internal/runtime` | `BenchmarkRuntimeExecuteJob` / `^BenchmarkRuntimeExecuteJob$` | 3 real task executions, each independently verified | Payload parsing, local witness lookup, proving admission, Prove, Base64, deadline gates, Result construction and result checks | Setup/load, network/persistence and final output decode/Verify |
| `./cmd/worker` | `BenchmarkWorkerExecuteJob` / `^BenchmarkWorkerExecuteJob$` | 3 direct handler executions, each independently verified | Production WorkerService handler: task construction, fresh task timeout, real proof, response, normal logging and checks | gRPC/mTLS, outer lifecycle admission wrapper, Coordinator, persistence, setup and independent output Verify |

Sources: [zk benchmarks](../internal/zk/preimage_bench_test.go),
[runtime benchmark](../internal/runtime/zk_preimage_bench_test.go),
[Worker benchmark](../cmd/worker/execution_bench_test.go).

Load results may reflect a warm OS page cache, not cold-storage startup. The
runtime parent has no shared deadline; the Worker creates a fresh task deadline
per call. These avoid one parent deadline expiring across calibration; they do
not remove production result-deadline checks. Metrics use the normal Noop path.
None of these benchmarks implements an external hard memory/wall supervisor.

The Prove wrapper includes witness creation and serialization. It is **not** the
same timed closure as P4's direct/pure Groth16 Prove on an existing witness.

## P3-B1 end-to-end benchmark

Entry: `./cmd/coordinator`, `BenchmarkSubmitJobEndToEnd`.
Exact `-bench`: `^BenchmarkSubmitJobEndToEnd$`; no extra environment enable flag.
The source recommends fixed `-benchtime=20x` and rejects b.N greater than 1000.
The cap does not eliminate Go calibration invocations or replace external limits.

The [fixture](../cmd/coordinator/execution_bench_test.go) owns a temporary SQLite
database, ephemeral mTLS identities, one Coordinator, one test Worker endpoint
with production Task/runtime proving, and a ready Client connection, all inside
one Go process. It performs fresh Setup outside timing. It is not the production
WorkerService process: Worker startup, heartbeat/lifecycle wrapper and recovery/
liveness loops are not included. The Coordinator opens a new dispatch connection
per attempt; the Client connection is warm.

There are **three successful warmup jobs**. Timed Go windows wrap submission work:
request/context creation, the real gRPC SubmitJob call and response processing.
The RPC includes mTLS Worker dispatch, real proving, independent Coordinator
verification and terminal SQLite Save before response. A separately recorded
`RPCNanoseconds` interval wraps the RPC itself; do not equate it exactly with the
outer Go timing window.

Post-response SQLite Load/audit, metrics Gather, result JSON and fixture cleanup
are outside Go timed windows. `ObservationNS` includes audits; it is not the sum
of RPC latencies. Allocations cover the whole process during timed windows, not
just the Client. Prometheus registries run without an HTTP exporter.

The no-fault result must have one accepted/verified attempt per job, durable
Succeeded with the full spec, zero Busy, and matching metric/call accounting.
`P3B1_RESULT` separates warmup and measured rows and records invocation identity
and b.N; preserve calibration invocations instead of merging by reused JobID.
Budgets include task 30s, lease 45s, RPC 120s, readiness 15s and cleanup 10s per stage.
These are fixture settings, not changed production defaults.

This measures local RPC plumbing and execution, **not networked multi-host
deployment**. Output/accounting and fixture lifecycle are part of its contract.

## P3-B2 capacity benchmark

Entry: `./cmd/coordinator`, `BenchmarkSubmitJobCapacity`.
Select exactly one child, e.g. `^BenchmarkSubmitJobCapacity$/^C$`.
The other valid exact child selectors replace C with A, B or D.

| Child | Workers | Proving capacity per Worker | Closed-loop clients |
|---|---:|---:|---:|
| A | 1 | 1 | 1 |
| B | 1 | 1 | 2 |
| C | 1 | 2 | 2 |
| D | 2 | 1 | 2 |

`DZKR_BENCH_PREIMAGE_DIR` must name an externally prepared temporary directory
containing matching `proving.key` and `verifying.key` for the production circuit.
The [loader](../cmd/coordinator/execution_bench_fixture_test.go) hashes both files,
checks the 331-constraint circuit and performs **one extra Prove + Verify key-pair
preflight outside timing**. Workers load independent prover instances from that
same pair. Do not generate new keys per Worker or mix pair identities.

Each selected child owns a fresh fixture. **Six successful warmups** run before
measurement; two Workers each handle at least three. Measured clients share one
ready Client connection, issue a bounded closed-loop cohort, and wait for joins.
At most 1000 requested jobs are allowed. The cohort watchdog is 120s; cleanup uses
shared Worker deadlines and reports incomplete joins rather than claiming success.
Coordinator task/lease/RPC settings match the P3-B1 fixture.

Go `ns/op` is the coordinated cohort and client-join timed interval divided by
b.N, not individual RPC latency. Throughput uses successful durable outcomes over
the recorded observation interval. Reciprocal throughput is not per-request
latency. Durable audits, metrics Gather, cleanup and JSON reporting occur outside
Go timing. A watchdog-triggered cancellation/drain remains part of the observed
failure path, not silently excluded successful work.

`P3B2_RESULT` distinguishes infrastructure Busy exhaustion (durable Failed) from
task failure, cancellation, unresolved/censored work and rejected results. It
checks attempts, actual proof calls, finished outcomes, metrics and durable specs;
overload need not produce 100% success to be valid accounting. Valid and comparable
are separate result fields. Half-open proof-call lifetime intervals establish
overlap and capacity bounds, **not simultaneous CPU execution**.

Sources: [capacity benchmark](../cmd/coordinator/execution_capacity_bench_test.go),
[capacity controls](../cmd/coordinator/execution_capacity_test.go),
[cleanup controls](../cmd/coordinator/execution_bench_cleanup_test.go).
All endpoints share one process CPU/GC budget. This is not a multi-host or
multi-process throughput result, and it omits production Worker lifecycle/heartbeat.

## P4-B1 experimental correctness

Package: `./internal/zk/experimental/mimcchain`, not production task dispatch.
The [circuit](../internal/zk/experimental/mimcchain/circuit.go) hashes one field
element with fresh MiMC state per link. N is fixed circuit metadata, not a witness;
each length needs its own matching setup pair. No runtime TaskType selects N.

Ordinary `TestChainLengthValidation`, `TestCircuitRelations` and
`TestReferenceConcurrent` cover allowed lengths, N=1 compatibility, native digest
agreement, private/public separation and N=1,2,8 Solve positive/negative cases.
N=32 is reserved for compilation calibration.

| Entry / exact `-run` selector | Gate | Actual crypto/compile budget |
|---|---|---|
| `TestCompileCalibration` / `^TestCompileCalibration$` | `DZKR_MIMCCHAIN_CALIBRATE=1` | Compile N=1,2,8,32 once each; no Solve/Setup/Prove/Verify |
| `TestGroth16N2` / `^TestGroth16N2$` | `DZKR_MIMCCHAIN_GROTH16=1` | Compile N=2, **2 Setup**, **1 successful Prove**, **2 successful Verify**, **2 expected Verify rejections** (wrong digest and unrelated VK) |

These are gated experimental tests, not default Groth16 work. Calibration reports
allocation increments around Compile and cumulative process high-water RSS,
not isolated per-length peak memory or a Go benchmark sample. The correctness
test serializes/reloads keys and creates an unrelated second setup for a negative
control. It must not replace the CPU pilot when budgeting expensive operations.
See [calibration source](../internal/zk/experimental/mimcchain/calibration_test.go)
and [correctness source](../internal/zk/experimental/mimcchain/circuit_test.go).

## P4-B2 N=2 CPU pilot

The accepted historical run is `pilot-n2-reconciled-20261017T121714Z`, reviewed in
`review-n2-20261017T123541Z`: **VALID N=2 PILOT / PASS WITH FOLLOW-UPS**. Those are
external archive identifiers, not a new run or statistically stable baseline.
Historical evidence lives under `~/benchmark-archives/DistributedZKRuntime/`
(`p3-b2`, `p4-b1`, `p4-b2`); this layout is optional local evidence storage, not a
prerequisite for ordinary repository builds/tests. Never rewrite sealed history.

The [pilot source](../internal/zk/experimental/mimcchain/pilot_n2_test.go) uses
`TestMiMCChainCPUPilot/N2`, exact `-run` `^TestMiMCChainCPUPilot$/^N2$`, with
`DZKR_MIMCCHAIN_CPU_PILOT=1`, `DZKR_MIMCCHAIN_CPU_N=2`, benchtime1x and effective
GOMAXPROCS4. The independent process selects `^TestMiMCChainCPUArtifactAudit$`
with `DZKR_MIMCCHAIN_CPU_AUDIT=1`. All evidence/run/source/barrier prerequisites
are in [the exact gate contract](testing.md#experimental-opt-in-gates).

| Process | Setup | Successful Prove | Successful Verify | Wrong-digest Verify rejection |
|---|---:|---:|---:|---:|
| Pilot | 1 | 2 | 2 | 1 |
| Independent artifact audit | 0 | 0 | 1 | 0 |

Thus successful verification is **pilot 2 + audit 1**, not three pilot Verifies.
The pilot has one explicitly recorded preflight Prove/Verify pair and one timed
Prove/Verify pair. There are **no hidden warmups**. Its 17-operation ledger also
records untimed digest, public-witness construction, CCS serialization, negative
Verify and artifact I/O.

Ten single-operation timed samples:

| Operation ID | Timed closure |
|---|---|
| `compile` | Construct N=2 circuit and Compile BN254 R1CS |
| `witness` | Construct full witness from the prepared digest |
| `setup` | Groth16 Setup on compiled CCS |
| `pk_serialize` | PK WriteTo an in-memory buffer |
| `vk_serialize` | VK WriteTo an in-memory buffer |
| `pk_decode` | Allocate a proving-key object and ReadFrom serialized memory |
| `vk_decode` | Allocate a verifying-key object and ReadFrom serialized memory |
| `prove` | Direct Groth16 Prove with existing CCS/reloaded PK/full witness |
| `verify` | Direct Groth16 Verify with existing proof/reloaded VK/public witness |
| `proof_serialize` | Proof WriteTo an in-memory buffer |

The [test-only driver](../internal/zk/experimental/mimcchain/pilot_driver_test.go)
uses `testing.Benchmark` and `b.Loop()`. It requires explicit `1x`, one callback,
benchmark N=1 and exactly one started/completed operation. Extra iterations,
callbacks, errors or panics invalidate the sample without retrying the operation.
Counters/ledger writes are outside the timed operation; minimal loop/timer call
overhead remains. Go's pre-callback GC is outside the timed closure. Chain N=2
and benchmark iteration N=1 are different quantities.

The pilot binds source manifest/run/fixture identities and artifact SHA256/size
for CCS, PK, VK, proof and public statement. The launcher records binary and tool
identity. The separate audit reloads the stored material, checks CCS metadata,
recomputes the expected public digest and performs exactly one real Verify;
its two ledger operations are untimed. The Python numerical/resource audit is
separate from that Go cryptographic audit and adds **zero** Verify calls.

The [start barrier](../internal/zk/experimental/mimcchain/pilot_start_barrier_test.go)
precedes fixtures and timed work. The reviewed supervisor checks owned
PID/start-ticks/executable identity and Unix peer credentials, takes a live
readiness sample, releases the target and observes acknowledgement. It surrounds
the corrected watchdog with bounded cleanup. Historical launch settings included
inner wall 210s, outer wall 230s, TERM/KILL grace 0.15/0.5s and target test timeout 3m;
the runner has separate CPU/phase/resource stops. Those bounds are historical
tool settings, not portable production controls or a new execution authorization.
Those are historical N=2 launcher settings. The maintained `tools/bench` package
now supplies policy/source binding, raw group RSS, G4 supervision and validation,
and the D2 outer Collector for N=8. Archived scripts may use fixed paths or write
derived evidence and must not be replayed in place.

## Resource evidence and historical tooling follow-ups

| Term | Meaning / limitation |
|---|---|
| Pre-release readiness RSS | Live target observation before release; proves observer readiness, not peak workload memory |
| Post-release live workload RSS | Valid target samples after release, bound to process/executable identity |
| Observed sampled peak | Maximum of retained valid samples; can miss spikes between samples |
| Process-group RSS summary | Runner's member-aggregate sampled maximum; different population/cadence from target-only observations |
| Lifetime peak | Unknown unless independently established; neither missing samples nor readiness-only observations establish it |
| Hard memory enforcement | Not provided by `/proc` sampling, polling stops or a Go memory hint |

The reviewed N=2 run retained **nine pilot post-release live samples** and **zero
independent-audit post-release live samples**. The audit had a valid pre-release
readiness sample; its workload-period peak remains **unknown**, not zero. VALID
acceptance was limited to the declared sampled-resource contract, not proof of
complete peak-memory behavior, a cgroup bound or statistically stable performance.

The N=2 review originally identified two **Phase C** requirements:

1. Bind the exact policy SHA256 in the pre-execution launch record before spawn.
   Historical launch hashes/source timestamps and the final archive checksum do
   not establish that launch-bound policy commitment.
2. Retain timestamped raw process-group membership/identity and per-process RSS,
   including missing/excluded/error observations, so an independent auditor can
   recompute member totals and group peaks. The historical inner runner retained
   group peak summaries; outer raw target-only samples cannot replace them.

These were subsequently implemented in the maintained G2 policy/source binding
and G3 common raw RSS stream, with G4/G5 cross-artifact/resource reconciliation.
They were not retrofitted into the historical N=2 archive. The real synthetic Go
Barrier integration and the final tooling review were accepted before D1/D2.
Synthetic probes remain separately gated; `hold` needs external termination and
is not an ordinary test command.

## Accepted N=8 CPU pilot

The D1 N=8 entry/profile, D2 outer-watchdog/Collector and one real CPU-only BN254
MiMC Chain N=8 Groth16 execution are implemented and independently accepted.
The D3 verdict was **PASS WITH FOLLOW-UPS**, with zero acceptance blockers; P4-D2
and the bounded N=8 milestone are complete. N=32 remains optional and deferred.

| Metric | Historical N=2 | Accepted N=8 |
|---|---:|---:|
| Constraints | 661 | 2,641 |
| Setup | 54.184064 ms | 204.938119 ms |
| Prove | 9.516367 ms | 29.742667 ms |
| Verify | 0.500668 ms | 0.614877 ms |

Each value is one timed invocation (`benchmark N=1`), not a mean, median or minimum
of two calls. The driver and timed closures match after specializing the chain
length. Both pilots recorded Setup x1, Prove x2 (preflight plus timed), successful
Verify x2 and wrong-Digest rejection x1. The N=8 audit reconciled source-bound Go
verification records without an additional mathematical Verify. Dates, scheduling,
cache/GC and random setup differ; descriptive ratios are not a statistical scaling
law or a throughput result.

The accepted N=8 run retained one COMPLETE readiness sample, six COMPLETE workload
samples and a terminal no-live-members observation. Its common raw group RSS peak
was **21,484 KiB**; no observed resource-stop trigger or residual owned process
remained. This stream differs from the separate direct-target sampled peak
(about 22.090 MiB). Neither establishes the true lifetime maximum or hard memory
isolation. User-space watchdogs and cleanup are not hard real-time guarantees.

The required N=8 execution path is the accepted D2 Collector with frozen schema-2
profile `mimcchain-n8-groth16-v1`, exact source/binary/policy/argv/environment
bindings, fresh external paths/run ID, the real Go Barrier and one attempt with
no automatic retry. Budgets remain target/barrier/Supervisor/Guardian/outer
**3/2/8/15/25 seconds**, with one absolute outer deadline including seal/audit.
Launch requires fresh MemAvailable >=5 GiB and the other accepted host/ownership
gates; sampled stops do not supply a cgroup memory cap. No default test command
should launch this path.

External evidence identifiers are `p4-d2/n8-cpu-pilot-20261021T045408Z` and
`p4-d3/n8-evidence-review-20261021T052144Z` under the optional external archive
layout. They are not public URLs or portable executable configurations. Keep raw
traces, process records, keys and binaries external. Any future Groth16 pilot
requires new explicit authorization and fresh preparation, not replay of accepted
archives. See [tooling contracts](../tools/bench/README.md).

## Comparability and change control

- P3-A wrapper Prove includes witness/serialization; P4 pure Prove does not.
- P3-B1 RPC intervals and Go timing windows differ from P3-B2 cohort timing and
  throughput. Reciprocal throughput is not individual request latency.
- Local multiple endpoints share CPU/GC and do not measure multi-machine networks.
- One N=2 or N=8 observation per operation is a bounded pilot, not a stable statistical
  estimate. Do not publish it as a distribution or reliable speedup ratio.
- Preserve names, selectors, tags, environment, warmup, call budgets, timed
  closures, fixture/key lifecycle, source/build/dependency/CCS/key/proof hashes,
  tool versions and resource policy. HEAD alone does not identify locally
  untracked experimental inputs; the accepted archives retain explicit source
  identities independently of the eventual Git checkpoint.
- Changes to those inputs need a new measurement-contract version and a written
  difference record. A helper refactor retaining the same benchmark name is not
  automatically comparable. Further comparisons need a common reviewed contract,
  not rewritten historical results or an inference from unchanged entry names.

Documentation does not authorize another Groth16 pilot, N=32 execution, archive
changes or test/helper consolidation. GPU/CUDA is outside the current roadmap.
Further experimental execution needs separate reviewed scope and resource budgets.
