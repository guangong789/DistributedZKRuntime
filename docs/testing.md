# Current test execution contract

This describes the existing source, not a new test-tier implementation. No build
tags, test selectors, environment gates or CI jobs were added by this document.
For architecture see [README](../README.md); for expensive operations and timing
see [benchmark contracts](benchmark-contracts.md).

## What ordinary tests execute

With experimental enable flags unset, ordinary package tests and `go test ./...`
include deterministic state/lifecycle checks **and** real production N=1 Groth16
Setup/Prove/Verify, localhost gRPC/mTLS, temporary SQLite databases, and controlled
Unix signal subprocesses. Experimental default checks also Compile/Solve small
MiMC Chain circuits for N=1,2,8; they do not run experimental Groth16. Signal tests
skip their Unix-specific checks on Windows.

This is not a pure fast-unit-test suite. Tests create ephemeral keys, certificates
and database fixtures; they do not require deployment keys or private witness
files from the working directory. Some helpers perform Setup or generate a proof:
inspect their bodies before changing fixture lifetimes or reusing them in timing.

| Documentation category | Actual selection today |
|---|---|
| State/runtime/correctness regressions | Ordinary `Test` selection |
| Real integration and production cryptography | Also ordinary `Test` selection; not uniformly opt-in |
| Experimental calibration/correctness/performance | Exact enable flags and selectors below |
| Historical experiments | External archives; not ordinary test entry points |

Benchmarks need an explicit `-bench` selection. Avoid `-bench .`: several entries
perform real proving, and ordinary Go benchmark calibration may repeat fixtures
and expensive operations. Go test commands can also write caches; `-mod=readonly`
prevents module-file changes, not all filesystem activity.

## Protected regression contracts

The following files are examples of required protection, not deletion candidates.
Tests with similar fixtures often protect different execution boundaries.

| Contract | Representative source |
|---|---|
| P0: exhausted normal submission persists `Failed` only for its exact published generation; no-worker/no-attempt does not rewrite old state; storage errors remain errors | [submit_failure_test.go](../cmd/coordinator/submit_failure_test.go) |
| Ownership, concurrent AttemptID allocation, `max(memory, store)+1`, matching leases and retry limits | [attempts_test.go](../cmd/coordinator/attempts_test.go), [submit_test.go](../cmd/coordinator/submit_test.go) |
| Terminal persistence before response, full task specification and SQLite reopen | [terminal_persistence_test.go](../cmd/coordinator/terminal_persistence_test.go), [sqlite_store_test.go](../cmd/coordinator/sqlite_store_test.go) |
| Restart recovery, failed-session repair, cooldown and generation/spec reconciliation | [recovery_failure_test.go](../cmd/coordinator/recovery_failure_test.go), [recovery_schedule_test.go](../cmd/coordinator/recovery_schedule_test.go), [ZK restart integration](../cmd/coordinator/zk_preimage_recovery_integration_test.go) |
| Task `Cancelled` vs RPC errors vs parent cancellation; cancellation persistence and no retry after parent cancellation | [submit_cancellation_test.go](../cmd/coordinator/submit_cancellation_test.go), [Coordinator timeout tests](../cmd/coordinator/timeout_test.go) |
| P2: valid positive TimeoutMs bounds and strict result deadline, including after Prove; client job/RPC deadlines remain separate from lease | [runtime timeout tests](../internal/runtime/timeout_test.go), [Worker timeout tests](../cmd/worker/timeout_test.go), [Client budgets](../cmd/client/execution_test.go) |
| Lease expiry cancels only the child attempt; late/stale results and Verify crossing a deadline cannot commit | [result_acceptance_test.go](../cmd/coordinator/result_acceptance_test.go), [proving_transport_test.go](../cmd/coordinator/proving_transport_test.go) |
| Bounded proving admission, `ResourceExhausted`, and retaining the slot until actual Prove returns after cancellation | [runtime admission](../internal/runtime/zk_preimage_admission_test.go), [Worker admission](../cmd/worker/proving_admission_test.go), [Coordinator admission](../cmd/coordinator/proving_admission_test.go) |
| P1-A: Coordinator drains admission, joins handlers/dispatch/recovery before store close, and observes a shared shutdown budget | [Coordinator shutdown](../cmd/coordinator/shutdown_test.go), [Coordinator signal wiring](../cmd/coordinator/shutdown_signal_test.go) |
| P1-B: Worker cancels/joins execution and background clients, preserves proof ownership and closes the original listener | [Worker shutdown](../cmd/worker/shutdown_test.go), [background shutdown](../cmd/worker/shutdown_background_test.go), [Worker signal wiring](../cmd/worker/shutdown_signal_test.go) |
| Executor state transitions, duplicate admission, graceful/forced shutdown and blocked submit/result paths | [executor_test.go](../internal/runtime/executor_test.go), [job_test.go](../internal/runtime/job_test.go) |
| mTLS chain/EKU/IP/URI identity, exact method roles and WorkerID binding at all production RPC boundaries | [rpcauth tests](../internal/rpcauth), [Coordinator transport](../cmd/coordinator/transport_test.go), [Worker execution transport](../cmd/worker/execution_transport_test.go), [Client transport](../cmd/client/transport_test.go) |
| Strict public payload, legacy record safety, private/public witness separation and real proof acceptance/rejection | [payload parser](../internal/zk/preimage_payload_test.go), [recovery payloads](../cmd/coordinator/zk_payload_recovery_test.go), [real preimage proof](../internal/zk/preimage_test.go), [end-to-end witness flow](../cmd/coordinator/zk_preimage_integration_test.go) |
| Metrics reflect persisted transitions, bounded labels and actual proving/verification; fixtures do not imply stronger production coverage | [metrics integration](../cmd/coordinator/metrics_integration_test.go), [metrics backend](../internal/metrics/prometheus/metrics_test.go) |

`TestWorkerShutdownListenerClosureObservation` checks the **original listener's**
`Accept()` error with `errors.Is(err, net.ErrClosed)`. Its open-listener negative
control must fail observation even when subsequent cleanup closes that listener.
A failed connection to the old port, or a Serve return alone, is not equivalent.

Current retry semantics distinguish a valid task `Failed`/`Cancelled` response
(terminal) from an RPC `Canceled`/`DeadlineExceeded` while the parent is still live
(infrastructure failure, eligible for the existing retry path). Fencing protects
result acceptance, not exactly-once remote execution. Controlled reopen tests do
not establish power-loss durability or replicated-Coordinator correctness.

## Ordinary commands: deliberate execution only

The commands below are templates, not checks performed by this documentation
change. Use a machine with the declared Go toolchain, cached dependencies and
permission for local sockets, temporary files and child processes. In an offline
environment, missing cached dependencies should fail rather than download.

First sanitize the current shell for ordinary regression selection:

```sh
unset DZKR_MIMCCHAIN_CALIBRATE DZKR_MIMCCHAIN_GROTH16
unset DZKR_MIMCCHAIN_CPU_PILOT DZKR_MIMCCHAIN_CPU_AUDIT
unset DZKR_MIMCCHAIN_BARRIER_PROBE DZKR_MIMCCHAIN_START_BARRIER
unset DZKR_TEST_COORDINATOR_CHILD DZKR_TEST_COORDINATOR_ARGS
unset DZKR_TEST_WORKER_CHILD DZKR_TEST_WORKER_ARGS
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off
```

From the repository root, choose the intended scope; do not run every line by
habit. The package/repository commands include the real work described above.

```sh
# Small focused state regression.
go test -mod=readonly -count=1 -timeout=2m -run '^TestValidJobTransitions$' ./internal/runtime

# Ordinary package regression, including integration/cryptography.
go test -mod=readonly -count=1 -timeout=10m ./cmd/coordinator

# Ordinary repository regression, including real N=1 cryptography and signals.
go test -mod=readonly -count=1 -timeout=10m ./...

# Focused race check; this is not a substitute for all lifecycle/race coverage.
go test -race -mod=readonly -count=1 -timeout=2m -run '^TestProvingLimiter' ./internal/runtime

# Static Go analysis; may still build analysis inputs/write caches.
go vet -mod=readonly ./...
```

The `-timeout` values bound test processes, not production job/RPC/lease durations
or total multi-package wall time. They are not hard memory limits. Inspect failures
before rerunning; do not classify an unexplained failure as harmless because a
later run passes. The explicit development PKI checks in
[test_bootstrap.py](../scripts/dev-pki/test_bootstrap.py) are separate from Go test
selection and invoke OpenSSL and Go; they are not temporary ECS diagnostics.

## Experimental opt-in gates

**Expensive/synthetic experiment metadata, not casual launch instructions.**
The package is `./internal/zk/experimental/mimcchain`. Use exact selectors and a
reviewed, bounded supervisor with fresh external evidence. No environment flag
itself supplies supervision. Do not run archived launch scripts in place: they
can contain fixed paths and write into their historical evidence directory.

| Entry / exact `-run` value | Enable flag | Additional source requirements / behavior |
|---|---|---|
| `TestCompileCalibration` / `^TestCompileCalibration$` | `DZKR_MIMCCHAIN_CALIBRATE=1` | Compiles N=1,2,8,32 once each; no Setup/Solve/Prove. Includes N=32, so it is not an implicit authorization to scale an experiment. |
| `TestGroth16N2` / `^TestGroth16N2$` | `DZKR_MIMCCHAIN_GROTH16=1` | Real N=2 correctness, two setup pairs, one proof and positive/negative verification. Not the CPU pilot budget. |
| `TestMiMCChainCPUPilot` / `^TestMiMCChainCPUPilot$/^N2$` | `DZKR_MIMCCHAIN_CPU_PILOT=1` | Requires `DZKR_MIMCCHAIN_CPU_N=2`, `-benchtime=1x`, effective GOMAXPROCS=4, and evidence/run/source identity below. |
| `TestMiMCChainCPUPilot` / `^TestMiMCChainCPUPilot$/^N8$` | `DZKR_MIMCCHAIN_CPU_PILOT=1` | Requires `DZKR_MIMCCHAIN_CPU_N=8`, `-benchtime=1x`, effective GOMAXPROCS=4, and the frozen schema-2 `mimcchain-n8-groth16-v1` profile through the D2 Collector. Metadata only; a future run requires separate authorization. |
| `TestMiMCChainCPUArtifactAudit` / `^TestMiMCChainCPUArtifactAudit$` | `DZKR_MIMCCHAIN_CPU_AUDIT=1` | Separate process using the pilot's existing matching artifact directory and run/source identity; creates new audit files exclusively; one Verify, no Setup/Prove. |
| `TestMiMCChainStartBarrierProbe` / `^TestMiMCChainStartBarrierProbe$` | `DZKR_MIMCCHAIN_BARRIER_PROBE=1` | Synthetic, no crypto. `DZKR_MIMCCHAIN_BARRIER_PROBE_MODE`: empty/default returns, `trivial` checks tiny work, `nonzero` fails after release, `preexit` fails before handshake, `hold` waits indefinitely and requires external termination. |

One real N=8 pilot has been independently accepted; its original run and audit
remain external historical evidence. This does not enable another run. The N=8
path requires the accepted D2 Collector, fresh configuration/source/binary/policy
bindings and resource preflight; a bare Go selector is not a safe launch recipe.
The separate `TestMiMCChainCPUArtifactAudit` remains **N=2-only**. Python G4/N=8
evidence checking reconciles the recorded Go calls; it does not redo Groth16
Verify. See [the accepted N=8 contract](benchmark-contracts.md#accepted-n8-cpu-pilot).

For the pilot and the N=2 artifact audit:

- `DZKR_MIMCCHAIN_EVIDENCE_DIR`: absolute, outside the repository. The pilot leaf
  must not exist; the audit must use its matching existing leaf with no prior
  audit outputs. Both use exclusive creation; never point them at sealed history.
- `DZKR_MIMCCHAIN_RUN_ID`: pilot requires a nonempty value without `/` or `\`;
  the audit must match the stored run ID.
- `DZKR_MIMCCHAIN_SOURCE_ID`: pilot requires 64 hexadecimal characters; the audit
  must match the fixture. The historical launcher supplied the preflight manifest
  SHA256. This is a caller-supplied source binding, not automatic Git discovery.

For the supervised barrier contract, the supervisor sets
`DZKR_MIMCCHAIN_START_BARRIER=1`, `DZKR_MIMCCHAIN_BARRIER_SOCKET` to its private Unix
socket and `DZKR_MIMCCHAIN_BARRIER_TOKEN` to its generated 64-character token.
`DZKR_MIMCCHAIN_BARRIER_TIMEOUT` defaults to `5s`; a supplied duration must be
positive and at most `30s`. Pilot, audit and probe call this hook before fixture
work/timing. The Go hook is a no-op when START_BARRIER is unset: this does **not**
make an unsupervised pilot an acceptable measurement. Calibration and
`TestGroth16N2` do not call this barrier hook.

The accepted pilot/audit supervisor clears all five experimental enable flags,
then enables only the selected role. It sets `GOTOOLCHAIN=local`, `GOPROXY=off`,
`GOSUMDB=off`, `GOMAXPROCS=4` and `PYTHONDONTWRITEBYTECODE=1`; the historical launcher
also set `GOGC=100` and left `GOMEMLIMIT` unset. Its target used `-test.count=1`,
`-test.cpu=4`, `-test.benchtime=1x`, `-test.timeout=3m` in a prebuilt Go test binary.
The audit does not independently enforce CPU_N, GOMAXPROCS or benchtime in its Go
guard; those settings belong to the accepted supervised execution contract.
See [historical pilot and resource semantics](benchmark-contracts.md#p4-b2-n2-cpu-pilot).

## Signal subprocess variables are not opt-in flags

`TestCoordinatorSignalSubprocess` uses `DZKR_TEST_COORDINATOR_CHILD` and
`DZKR_TEST_COORDINATOR_ARGS`; `TestWorkerSignalSubprocess` uses
`DZKR_TEST_WORKER_CHILD` and `DZKR_TEST_WORKER_ARGS`. The parent test launches its
current test executable with encoded startup arguments; the child takes the real
main/signal path. Unsetting these variables does **not** skip the parent test.
Do not set child variables manually in an ordinary test shell.

## Proposed next phases, not implemented

A fast default tier plus explicitly selected mandatory integration/real-crypto
checks is a proposal, not today's selection behavior. Moving/gating tests would
need separate authorization and replacement execution coverage. Likewise, narrow
Setup/Load/Prove/Verify helpers may reduce repetition later; no fixture, assertion,
test or benchmark boundary was changed here. Preserve independent native digest
oracles and original-listener shutdown controls during any future consolidation.
