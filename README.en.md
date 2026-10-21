[简体中文](README.md) | English

# DistributedZKRuntime

A Go distributed job runtime with one Coordinator, multiple Workers, gRPC over
TLS 1.3/mTLS, and SQLite-backed job state. The current ZK workload proves a MiMC
preimage relation with Groth16 on BN254. Workers generate proofs; the Coordinator
independently verifies them before persisting successful completion.

## Execution model

```text
Client -- SubmitJob --> Coordinator -- ExecuteJob --> selected Worker
                           |                           |
                     SQLite JobRecord           local WitnessStore
                           |                           |
                     verify proof <--------------- Groth16 proof
                           |
                     persist terminal state --> Client response
```

- `SubmitJob` is synchronous: its caller waits for a terminal response or error.
  The Coordinator owns the JobID for that RPC and runs asynchronous attempt RPCs
  internally, with at most two attempts per execution session.
- Workers register and heartbeat. Scheduling is Coordinator-driven round robin
  over workers marked alive, not a Worker-pulled job queue. Liveness uses periodic
  checks; recovery combines readiness notifications with timed backoff/scans.
- Each attempt persists the task specification and a monotonic AttemptID before
  publishing its in-memory lease. Infrastructure failures or lease expiry can
  retry while the parent context remains live. Valid task `Failed`/`Cancelled`
  responses are terminal; parent cancellation stops the submission.
- Result identity and lease/generation fencing precede proof verification. Final
  acceptance checks run again before terminal persistence, protecting against
  cancellation or lease expiry during verification. This fences acceptance; it
  does not promise exactly-once execution on remote Workers.
- SQLite retains JobID, state, attempt, worker and task specification. Startup
  marks valid `Running` records `Recovering`; the recovery scheduler reuses the
  execution path. Worker registry, active ownership, leases and cooldowns remain
  in memory. Proof outputs are not persisted for later result retrieval.
- Worker proving concurrency is bounded by `ProvingLimiter`. A full Worker returns
  `ResourceExhausted`. Cancellation/deadline gates reject invalid late output but cannot
  preempt an ongoing Groth16 call; its proving slot remains owned until it ends.

This is a single-Coordinator implementation, not a replicated Coordinator or a
durable exactly-once task system. See the source in [cmd/coordinator](cmd/coordinator),
[cmd/worker](cmd/worker) and [internal/runtime](internal/runtime).

## Cryptographic workload

The production `zk_preimage_prove` task proves `MiMC(secret) == digest`: the secret
is private and the digest is public. The current BN254/R1CS circuit has **331
constraints** with the pinned dependencies. The public payload contains only
`witness_ref` and a decimal `digest`; the Worker resolves the secret from its local
witness store. Strict payload validation runs at the Coordinator and Worker.

[internal/zk](internal/zk) implements setup, key loading, proving and verification.
The separate [MiMC Chain experiment](internal/zk/experimental/mimcchain) supports
fixed lengths N=1,2,8,32 and is **not integrated into production task dispatch**:

| Size | Existing evidence |
|---|---|
| N=1 | Production-compatible digest and circuit correctness |
| N=2 | Real Groth16 correctness and an independently reviewed CPU pilot |
| N=8 | One real CPU Groth16 pilot, independently accepted; 2,641 constraints |
| N=32 | Compile-only calibration; further work is optional and deferred |

There is no production scalable-chain proving path. GPU/CUDA is not part of the
current project roadmap.

### Accepted CPU pilot

The single N=8 execution used the reviewed process supervision, frozen policy and
source bindings, Barrier synchronization, and G4/G5/D2 evidence checks. It ran
Setup once, Prove twice, successful Verify twice, and one incorrect-Digest Verify
that was rejected. No automatic retry was performed. The independent D3 audit
accepted the pilot with zero acceptance blockers (`PASS WITH FOLLOW-UPS`).

| Metric | Historical N=2 | Accepted N=8 |
|---|---:|---:|
| Constraints | 661 | 2,641 |
| Setup | 54.18 ms | 204.938 ms |
| Prove | 9.52 ms | 29.743 ms |
| Verify | 0.50 ms | 0.615 ms |

Each timing is one observed timed invocation, not a statistical mean of the two
Prove/Verify calls. The comparable timing closures permit descriptive comparison,
not confidence intervals, throughput claims or a predictive scaling law.

The N=8 common-sample group RSS peak was **21,484 KiB**, with one COMPLETE
readiness sample and six COMPLETE workload samples. No resource-stop trigger or
residual owned process was observed. Finite sampling does not establish the true
lifetime peak or a hard cgroup-enforced memory limit; user-space watchdogs are
not hard real-time guarantees.

The external evidence identifiers are `p4-d2/n8-cpu-pilot-20261021T045408Z` and
`p4-d3/n8-evidence-review-20261021T052144Z`; they are local archive references,
not public download links or build prerequisites. Full raw evidence and key
material remain outside Git. Any future Groth16 pilot needs separate explicit
authorization, fresh evidence paths and resource gates; the accepted run is not
an instruction to replay it. See [benchmark contracts](docs/benchmark-contracts.md).

## Repository layout

```text
cmd/client/                    Demo submission and mTLS client
cmd/coordinator/               Scheduling, leases, fencing, SQLite and recovery
cmd/worker/                    Execution service, heartbeat and shutdown
cmd/zksetup/                   Production preimage Groth16 key generation
internal/runtime/              Tasks, executor, deadlines, witnesses and admission
internal/zk/                   Production circuit, proof and payload validation
internal/zk/experimental/       Standalone MiMC Chain research and gated pilots
internal/rpcauth/               TLS identities and RPC authorization
internal/metrics/               Semantic metrics and Prometheus backend
internal/testutil/testpki/      Ephemeral test certificates
internal/workerheartbeat/      Heartbeat/re-registration protocol step
proto/                         Protobuf source and generated Go RPC bindings
scripts/dev-pki/                Explicit development PKI bootstrap and checks
docs/                          Test and measurement contracts
```

## Developer orientation

Use Go **1.27.1** as declared in [go.mod](go.mod). Dependencies include gnark,
gnark-crypto, grpc-go and modernc SQLite. From the repository root, an intentional
build check is `go build -mod=readonly ./...`. With an already populated module
cache, set `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off` to avoid downloads. Building
does not generate keys, provision witnesses or start services.

Running services additionally requires a matching preimage PK/VK pair, a trusted
CA and role/endpoint-matching mTLS certificates, Coordinator identity allowlists,
a Worker-local witness file and a writable SQLite path. Existing flags include
Coordinator `-listen`, `-db`, `-verifying-key`, `-allow-client`, `-allow-worker`;
Worker `-id`, `-advertise`, `-proving-key`, `-witness-file`,
`-max-concurrent-proofs`; and `-tls-cert`, `-tls-key`, `-tls-ca` on RPC programs.
The Worker witness-file default is `private/witnesses.json`; provision it explicitly
or override it. The Client is a fixed demo submission, not a general job CLI.

[cmd/zksetup](cmd/zksetup) generates keys under `zk-artifacts/preimage` by default;
it performs real Setup. [Development PKI tooling](scripts/dev-pki/bootstrap.sh)
requires explicit `--out`, OpenSSL 3, Go and Git. Neither is part of a build check.
Do not regenerate or overwrite an existing deployment's matching materials just
to run a test. RPC paths require mTLS; separate metrics HTTP listeners default to
loopback and are not the authenticated job RPC service.

## Tests and research workflow

Read [the current test execution contract](docs/testing.md) before running tests:
ordinary tests include real cryptography and integration fixtures. Read
[the benchmark contracts](docs/benchmark-contracts.md) before performance work;
experiments require explicit selection, budgets and resource supervision.
Historical evidence stays outside Git. Local witnesses, credentials, generated
keys, databases and binaries must not be committed.
