# Experimental process tooling

Linux-only, opt-in CPU experiment tooling. G1–G5, the real synthetic Go Barrier
integration, D1 N=8 entry/profile, and D2 outer-watchdog/Collector are implemented
and accepted. One real N=8 pilot was independently accepted by D3 with zero
acceptance blockers (`PASS WITH FOLLOW-UPS`); the bounded N=8 milestone is closed.
N=32 remains optional and deferred; GPU/CUDA is outside the current roadmap.

The sections below retain historical design scopes and their limits. The original
G1 validation used synthetic Python commands, without Go or Groth16. Neither
those records nor this documentation authorize a new pilot. Any future Groth16
run requires separate explicit authorization, fresh external evidence and resource
gates; real N=8 execution must use the accepted D2 Collector, not a bare selector
or an archived launcher. See the [accepted measurements and limits](../../docs/benchmark-contracts.md#accepted-n8-cpu-pilot).

## Components and origins

- `run_check.py`: byte-identical corrected P4-B1-F runner. Creates an owned
  session, monitors, bounds wall/phase/CPU work, escalates TERM/KILL, reaps its
  child and records primary, evidence and cleanup outcomes separately.
- `supervise.py`: P4-B2 barrier supervisor, adapted only for package/direct-script
  imports and repository-local runner/cwd paths. It verifies peer/process/image
  identity and a live target RSS sample before release, then observes ack.
- `processes.py`: unchanged PID/start-ticks/PGID/SID and executable observations,
  target RSS, session membership and verified group signaling.
- `resource_audit.py`: unchanged independent target-resource auditor, renamed
  from `resources.py`. Recomputes readiness/post-release sampled maxima and
  checks handshake/cleanup evidence; it never invokes the target or crypto.

`provenance.json` records **historical G1 source-promotion** origins, hashes and
adaptations; it is not the current G1–D2 source manifest. Later accepted source
identities belong to their frozen launch/review records. The promoted tools
appear project-authored; reviewed headers/imports show no identifiable vendored
component or third-party notice requiring additional handling. This is a
lightweight provenance check, not a legal assurance or a new license grant.
Historical archives stay unchanged. One-off launchers, integrity scripts and the
old defective runner are not promoted. Adapted sources have new identities;
historical N=2 results were **not** produced by these new copies.

## Synthetic validation

Run from the trusted repository root with normal Python (not `-O`), using a fresh
external evidence directory. Disable bytecode writes; no dependencies to install.

```sh
python3 -B -m unittest -v tools.bench.tests.test_pure
python3 -B -m tools.bench.tests.harness --out /tmp/dzkr-bench-g1-UNIQUE
```

Pure modules can be run in separate fresh Python processes. A combined suite is
order-sensitive: `test_pure` reloads base modules, while the derived Supervisor
retains imported function references. In a shared process, run the G4 interface
tests before `test_pure`, as in the accepted nine-module validation order. A
different order can fail function-identity assertions; retain and diagnose that
failure rather than treating a subsequent pass as proof of order independence.
This test-isolation limitation does not change the accepted runtime source.

The pure suite uses external temporary files. Set `DZKR_BENCH_TEST_OUT` to an
existing external directory to retain its fixtures. Process cases skip when
invoked without the harness. The harness owns only newly spawned synthetic
children; it uses a process-local subreaper and pidfds, runs cases serially with
15s outer deadlines (240s suite), monitors a maximum of eight descendants and
system headroom, and stops on the first failure. Preflight needs at least 4GiB
MemAvailable; the in-run floor is 2GiB. Fixtures fork at most one child each and
do not allocate large workloads. These are monitored bounds, not kernel memory
or process-count caps. The independent harness checks/reaps adopted children;
cases also inspect `/proc` independently of runner cleanup reports. The unsafe
historical teardown fallback using a potentially reused numeric PGID is absent.

Tests cover zero/nonzero exit, live descendants, TERM resistance, timeout,
monitor exception, unrelated-process negative control, real Python-peer barrier
exchange, invalid identity, broken IPC and short-process readiness. These original
G1 process tests build or execute no Go Barrier binary; later accepted G4 coverage
is described below. Any future process check still needs separate authorization.
No deliberate session escape or supervisor SIGKILL probe runs.

## Preserved interfaces and limitations

Original flag names remain: runner `--out --label --cwd --wall-seconds
--term-grace --kill-grace -- ARGV...`; supervisor `--out --barrier --inner --outer
--role --probe-mode -- ARGV...`. Both support `python3 -B -m tools.bench.MODULE`
and direct script usage. `--help` is safe. The supervisor CLI only enables the
synthetic gate for role `probe`; `--role pilot` alone is not a pilot launcher.
There is no new maintained experiment-launch wrapper in G1.

The runner retains 50ms sampled group checks, historical CPU/phase/wall limits,
resource formulas and output schema. Outer target sampling remains distinct.
Readiness is not workload peak; missing post-release observations mean unknown,
not zero. G2 policy binding is provided by the separate probe adapter below; raw
member-level group evidence and improved missing-member classification are not
enabled in these legacy paths; the separate opt-in G3 adapter is described below. The runner's known
silent missing-RSS/vanished-member limitations remain. No hard memory cap exists.

The unchanged auditor is host-local: the recorded executable must still exist
with matching device/inode/hash, and the historical peer UID must match the
auditor UID. Its assertions require non-optimized Python. It does not provide
portable archive verification, policy enforcement or group-peak reconstruction.

Group signaling requires the retained original anchor. Deliberate setsid/setpgid
escape, supervisor SIGKILL, host failure and uninterruptible kernel/filesystem
operations remain outside the cleanup guarantee. Do not replay tools against
sealed archives, existing services or private deployment material. No new N=2
measurement, comparability claim or N=8 approval follows from G1 validation.


## G2: frozen resource policy binding

The maintained `policy_launch` adapter is deliberately limited to cooperative,
non-cryptographic **probe** targets. It wraps the unchanged supervisor API, which
spawns the approved runner CLI. This is not the historical N=2 launcher or a CPU
pilot authorization. The project mainline is reliable distributed Groth16 plus
scaled MiMC Chain CPU experiments. G3/G4/G5 and the single real N=8 run passed their
later gates; this G2 probe adapter remains noncryptographic and is not a pilot launcher.

`policies/synthetic-v1.json` is a complete version1 example. Every field is
required; unknown/duplicate keys, non-finite numbers, bool-as-number, missing
limits and unsupported versions fail closed. The policy maximum is16KiB.

| Policy field | Actual consumer | Validation |
|---|---|---|
| `schema` | G2 parser/verifier | integer1 |
| `wall_seconds` | supervisor `inner_seconds` -> runner `--wall-seconds` | 0.01..210 |
| `barrier_seconds` | supervisor barrier deadline | 0.01..2, <=wall |
| `outer_seconds` | supervisor outer deadline | <=230, >=wall+5 |
| `term_grace`, `kill_grace` | supervisor's runner argv | fixed0.15/0.5 |
| `phase_seconds` | runner PHASE_BUDGETS | compile30/setup90/setup_wrong_vk90/prove60 |
| `cpu_seconds`, `core_bytes` | runner pre-exec RLIMIT_CPU/CORE | fixed[180,190]/0 |
| `rss_stop_kib`, `available_stop_kib` | runner sampled stops; same2GiB supervisor guard | fixed2097152 each |
| `stop_escalation_kib` | runner escalation eligibility | fixed1048576 |
| `kernel_memory_limit` | honest evidence declaration | false; no hard memory cap |

Fixed fields are checked against the pinned source contract, not adjustable knobs.
The adapter uses an allowlisted environment, re-applies only the probe gate through
the supervisor's existing `env_overrides`, and records no inherited secret values.
It is single-threaded, like the runner's pre-exec contract. Public argv is recorded
exactly: callers must not put credentials or witnesses in command arguments.

`prepare` reads a regular single-link non-symlink file through a bounded descriptor,
compares two reads and fd/path identity/metadata, and rejects observed concurrent
change. It writes the **original bytes** into a new0700 run directory with exclusive
0400 files, validates the snapshot and records SHA-256/size. Only that frozen
snapshot's parsed values control invocation. Later replacement of the original
input is intentionally allowed and irrelevant; it is never re-read. Snapshot or
source substitution before launch is rejected. Use a trusted output parent and
trusted repository/interpreter. This resists other UIDs, not malicious same-UID
code, root, host failure or deliberate process-session escapes. No privileged
immutability or external attestation is claimed.

The immutable-in-use `manifest.json` binds run ID, snapshot, values, exact argv,
fixed relevant environment, cwd, tool source copies/hashes and directory identity.
Its `prepared` state proves intent only. Exclusive `execution.json` records a
one-shot launch request; it is not proof of spawn. `outcome.json` is reconstructed
from runner/supervisor disk evidence, separating observed spawn, spawn failure,
completion, runner failure/stop, cleanup failure and incomplete evidence. A crash
before finalization stays incomplete. Existing runner evidence formats are unchanged.

The read-only verifier hashes archived snapshots, source copies and output records,
reconstructs argv, compares actual budgets/environment and runner limits, checks
basic trace/cleanup consistency and rejects outcome contradictions. No target,
runner, supervisor or crypto is executed; the original policy path and current
executable files are not needed. Current G2 reads are bounded to2MiB per evidence
file. It is a **local consistency/integrity check**, not proof against coordinated
rewriting by the archive owner and not G3 RSS recomputation. Historical N=2 does
not acquire G2 compliance retroactively.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest -v tools.bench.tests.test_policy
# New trusted external directory; target is the existing harmless Python peer.
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -B -m tools.bench.policy_launch \
  --policy tools/bench/policies/synthetic-v1.json --out /tmp/dzkr-g2-UNIQUE -- \
  /usr/bin/python3 -B "$PWD/tools/bench/tests/fixtures/peer.py" trivial
python3 -B -m tools.bench.policy_verify /tmp/dzkr-g2-UNIQUE
```

Launch/verify exit0 means verified completion; exit1 means recorded non-success;
exit2 means invalid/rejected/incomplete required evidence. Verifier `--prepared`
checks only unlaunched intent and explicitly returns `run_succeeded:false`.
Do not run the full lifecycle suite to test G2. A real synthetic check requires
an independent bounded guardian as in G1; pure tests never spawn or signal.

## G3 v1: raw process-group RSS evidence

`group_run` is an opt-in adapter around the approved Runner's existing `monitor`
callable. It leaves the Runner, Supervisor, Observer, legacy auditor, G2 parser,
launcher/verifier and G1-F1 classification byte-identical. G2 launch does not
enable G3, and no historical experiment acquires this contract retroactively.

The path is `group_rss.observe -> Recorder JSONL -> group-summary.json ->
group_audit.audit`. One observation supplies both raw member evidence and the
Runner's RSS/MemAvailable resource checks. There is no second group sampler:
the adapter returns this sample's sum in KiB to the Runner. An incomplete or
invalid monitor observation raises a monitoring error and the unchanged Runner
performs its normal cleanup. It never substitutes zero for missing RSS.

The `g3-raw-group-rss-v1` contract records version, run ID, sequence, monotonic
start/end, phase/event, sampling site, expected PID/start-ticks/PGID/SID, anchor
observations, opening/closing membership scans, per-member before/after identity,
RSS bytes or null, observation status and error codes, group total or null and
MemAvailable. It does not collect command lines or environment secrets.

Discovery reads numeric `/proc` entries twice, bracketing member identity ->
VmRSS -> identity observations. PGID and SID must match the retained session
anchor. This is a bounded interval observation, not an atomic snapshot. A member
that appears and disappears between scans can remain unobserved. Unreadable
unidentified PIDs conservatively make discovery partial because their membership
is unknown. PID reuse across a short stream is conservatively rejected in v1.

| Status | Meaning | Complete-sample peak eligible |
|---|---|---|
| COMPLETE | Consistent discovery and all discovered live members measured | yes |
| PARTIAL | Changed membership, missing/unreadable/vanished data or scan errors | no |
| INVALID | Ownership or identity contradiction; auditor rejects the stream | no |
| NO_LIVE_MEMBERS | Both successful scans empty and anchor absent in both reads | no |
| ZOMBIE_ONLY | Only identified zombie/dead members; no RSS read or fabricated zero | no |

No-live is observed absence within those scans, never authority to signal a
numeric PGID later. PARTIAL retains known values with a null group total; it
cannot contribute to a complete peak or complete workload-sample coverage.
Zero in the Runner's legacy monitor interface is allowed only for verified
no-live/zombie observations, never for unreadable members.

The auditor streams and independently sums integer bytes, verifies member and
scan identities, sequence/time, explicit phase events, per-sample totals, raw
SHA/size and summary peaks. When a same-event legacy comparison is requested,
it also reads `inner/result.json` and checks the actual published Runner peak
and owner. It never imports the producer/Runner, invokes a target or repairs
records. Invalid input exits2; a structurally honest partial stream can be
audited but is explicitly not complete evidence.

Nominal sampling cadence remains the Runner's50ms sleep plus observation/loop
overhead; there is no extra polling thread. Records include observation duration,
and the summary reports sample count and maximum between-sample gap. Bounds are
256 members/sample,65,536 scanned PIDs,64KiB/proc read,256KiB/JSONL record,
16MiB/stream,8,192 records, and65,536 remembered stream identities. RSS and
MemAvailable use nonnegative integer bytes bounded by signed64-bit range.
Storage/write/close/limit failures mark storage incomplete or fail the adapter;
they cannot yield successful complete evidence. Kernel memory caps and protection
against blocking filesystem I/O remain outside this contract.

The CLI captures `unknown` phase unless an embedding caller supplies explicit
identity-bound readiness/release-ack/cleanup events through `phase_provider`.
It never guesses workload phase from elapsed time or stdout. `g3-result.success`
means command/recording/audit/cleanup success; callers must separately require
`workload_coverage == all_samples_complete_not_lifetime_peak` before claiming
complete *sampled* workload coverage. `unknown` or readiness-only captures leave
workload coverage `unobserved`. The new guarded synthetic fixture uses a private
file barrier to test this API with one optional child; it does not validate or
replace the Go/G2 barrier integration.

Readiness, workload, cleanup, unknown and Runner-monitor peaks remain separate.
Only the G3 adapter's same monitor events are numerically comparable with its
published legacy Runner peak. Old G2/Supervisor target RSS, other samplers and
historical N=2 peaks are different streams. Group sums may double-count shared
pages; they are neither PSS nor unique physical memory. Even all-COMPLETE samples
do not prove the true lifetime peak, continuous coverage or a hard memory limit.

Pure checks and evidence-only audit:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest -v tools.bench.tests.test_group_rss
PYTHONDONTWRITEBYTECODE=1 python3 -B -m tools.bench.group_audit RAW_JSONL SUMMARY_JSON
```

Actual process validation requires an independent bounded guardian with verified
identities, pidfds, process-local subreaper and no pre-existing children. The
`tests.group_process` helper refuses unguarded execution. Do not replay a run
into sealed evidence. G3 itself grants no execution authorization; later G4/G5 and
N=8 acceptance does not authorize future CPU runs.
### G4-F0: historical interface preparation

`g4_supervise.supervise(..., entrypoint='legacy')` delegates to the unchanged
`supervise.py`. G2 v1 policy preparation and verification still pin that legacy
file; neither its source identity nor its manifest/argv rules are relaxed.

The explicit `entrypoint='g4-bootstrap-v1', config=...` branch selects the separately
versioned Supervisor in `g4_supervise.py`. It retains the reviewed loop and calls
the legacy `finish` cleanup; only the bound Runner entrypoint/environment and
coordination hooks differ. This duplicates the small Supervisor body deliberately
to preserve the pinned G2 source, rather than silently substituting a new hash.

`g4_binding.prepare(policy_path, external_out, target_argv, run_id)` requires the
caller's single run ID and freezes `g4-runner-interface-v1`: exact policy bytes,
fixed G2 limits, source copies/hashes, Python/target executable identities,
environment, output paths and actual bootstrap argv. The sole approved command is
`PYTHON -B -m tools.bench.g4_bootstrap --config ABSOLUTE_CONFIG`; no shell command
or arbitrary module selection is accepted. The bootstrap validates live bindings
and calls unchanged `run_check.run_command` with the existing G3 sampler/Recorder.

Supervisor owns `/tmp/dzkr-g4-RUN_ID/events.sock` in a fresh 0700 directory. Both
peers check `SO_PEERCRED` against the owned Runner or its kernel parent and recheck
process generation. A confirmed handshake precedes target creation. Five bounded,
globally sequenced events carry the same run ID, target identity and Python
monotonic clock: verified ready, persisted readiness, release initiation, verified
acknowledgement, persisted workload. Supervisor waits for a checked raw-file
receipt before release; the bootstrap cannot label workload before verified ack.
Frames are at most 2048 bytes, the exchange shares a finite barrier deadline,
and missing/replayed/wrong-owner events fail closed. No stdout synchronization,
second sampler for enforcement, new signal ownership or legacy fallback exists.

F0 tests use mocked process/kernel boundaries and in-memory connections; they do
not run a target or Go. `bootstrap.json` distinguishes `execution_succeeded`,
`interface_complete` and `accepted`, and explicitly records that full G4 evidence
has **not** been assessed by Bootstrap alone. Real local IPC/Go-barrier integration,
the cross-artifact G4 validator, G5 review and N=8 proving were accepted in later
stages; those outcomes do not strengthen the narrower original F0 test scope.

## G4 synthetic Go integration

`g4_launch.py` selects the accepted `g4-bootstrap-v1` path and reuses the existing
harness's process-local subreaper/pidfd emergency cleanup. It consumes a fresh,
externally frozen `g4_binding.prepare` configuration exactly once. The default
synthetic policy remains wall=1s, barrier=1s, Supervisor outer=8s; the independent
harness envelope is 15s/8 descendants, with 4GiB initial and 2GiB live headroom.
No crypto workloads or full repository build are implied.

Build only `tests/fixtures/go_barrier/main.go` to an external path. It uses the
original Go `ready -> release -> released` wire protocol, touches 1MiB and stays
alive for 300ms after ack for observation. This hold is not benchmark work.
Run from the repository root with `PYTHONDONTWRITEBYTECODE=1` and Python `-B`:

```sh
python3 -B -m unittest -v tools.bench.tests.test_g4_verify
python3 -B -m tools.bench.g4_launch --config /external/fresh-run/g4.json
```

After the attempt, `g4_verify.seal(out)` indexes the fixed required artifact set;
`python3 -B -m tools.bench.g4_verify /external/fresh-run` independently checks
policy/source/image/argv/environment, authenticated event transcripts, receipts,
G3 raw RSS, published peaks, exit codes and cleanup. CLI 0 means accepted, 1 means
valid unsuccessful execution, and 2 means invalid/incomplete evidence. Mutable
owner-created hashes are consistency checks, not same-UID/root attestation.
Live validation requires the frozen source/executable installation still present.
Raw observed environment replaces the ephemeral barrier token with its hash.
Supervisor target RSS is distinct from the common G3 Runner-decision stream.
Neither stream is a lifetime maximum, unique physical memory, or a cgroup limit.
A failed positive attempt must be preserved; another attempt requires separate
authorization. G5 and all real proving pilots remain separately gated.

## N=8 crypto preparation (accepted; future execution needs authorization)

`TestMiMCChainCPUPilot/N8` explicitly requires `DZKR_MIMCCHAIN_CPU_N=8`,
`benchtime=1x`, and `GOMAXPROCS=4`. The N=2 selector remains supported. Both
use the original Go Barrier before fixture creation and the same 17-operation
plan: one Setup, two Prove calls, two successful Verify calls, and one
wrong-digest rejection. N=8 adds profile/curve/backend metadata; it does not
extend the separate N=2 artifact-audit target. No sleeps were added.

The optional `g4_binding.prepare(..., profile="mimcchain-n8-groth16-v1",
build_manifest=<path>)` creates schema 2 / `g4-bootstrap-crypto-v1`. Schema 1
remains the strict synthetic probe profile; environment variables alone cannot
select crypto. The event protocol, Bootstrap monitor, G3, original Runner,
and original Supervisor are unchanged.

The build record has exactly `schema`, `profile`, `sources`, `binary`,
`toolchain`, `go_version`, `argv`, `cwd`, `environment`, and `exit_code`.
Source entries are `{bytes, sha256}` for `g4_crypto.GO_SOURCES`, pinned to this
reviewed revision. Binary/toolchain entries use `g4_binding.executable`.
Record the actual offline compiler command from `g4_crypto.build_argv`, its
strict integer zero exit, repository cwd, and local/offline/GOMAXPROCS=4
build environment. The record and Go source snapshots are frozen with the
policy, exact N8 argv, executable identity, role `pilot`, and one run ID.
This is local provenance consistency, not adversarial build attestation.

The crypto profile fixes target/barrier/Supervisor/Guardian/outer budgets to
3/2/8/15/25 seconds, immediate MemAvailable to at least 5 GiB, one target and
one attempt. Fixed 2 GiB RSS/headroom stops and 1 GiB sampled-peak acceptance
are unchanged; they are not hard memory caps. The 25-second independent
parent watchdog and its owned-child cleanup are implemented by the accepted D2
Collector below. Preparation itself starts no target and does not authorize one.

`g4_verify` requires the ordinary G4/F1 identity, resource, coverage, exit and
cleanup gates plus the full N8 ledger, timing samples, footer, metadata and
artifact hashes. Its crypto check reconciles recorded calls in the bound Go
program; Python does not perform Groth16 verification. Pure fixtures are
explicitly synthetic and do not establish proving correctness or feasibility.
Historical bundles are not rewritten to match changed source hashes; their
original source versions and independent acceptance records remain authoritative.

## D2 outer adapter (accepted; future execution needs separate authorization)

`d2_collect` adds a fixed call boundary around the accepted Guardian and G1
`tests.harness.cleanup`; it does not replace their lifecycle implementation.
The single real N=8 D2 run completed without automatic retry. D3 accepted the
source-bound Go ledger (Setup x1, Prove x2, successful Verify x2, wrong-Digest
rejection x1), Barrier/RSS coverage, resource decisions, exits and cleanup.
The evidence remains external under the D2/D3 archive identifiers documented in
the benchmark contracts; this is not a new launch authorization or portable config.
Only a fresh external `p4-d2/n8-cpu-pilot-<UTC-id>/g4/g4.json` crypto config is
eligible. It accepts no arbitrary command. After the existing D1 freeze step,
prepare adds `outer-intent.json` and private source snapshots without launching:

```sh
python3 -B -m tools.bench.d2_collect prepare --config "$D2_CONFIG"
```

The frozen intent contains the **exact** canonical Python/run argv, cwd, complete
environment, Guardian/verification argv, config hash, source/helper hashes and
25-second budget. A separately authorized execution must use those values:

```sh
# Do not execute during G1. D2_CONFIG must name the separately authorized fresh config.
python3 -B -c '
import json,os,sys
from pathlib import Path
intent=json.loads((Path(sys.argv[1]).parent/"outer-intent.json").read_bytes())
os.chdir(intent["cwd"])
os.execve(intent["adapter_argv"][0],intent["adapter_argv"],intent["environment"])
' "$D2_CONFIG"
```

The adapter requires exact runtime argv/environment and rechecks bindings before
launch and after verification. It locks the D2 target slot, checks immediate
5 GiB MemAvailable/2 GiB disk/CPU/load/no competing experiment, enables a
process-local subreaper and exclusively reserves `outer-attempt.json` before
its sole Guardian launch. A failed spawn also conservatively consumes that
reservation; no retry, probe or fallback is provided. Preflight rejection is
exit 2 and creates no attempt marker. No actual D2 marker is created by tests.

One monotonic 25-second window starts before reservation and covers Guardian
import/preflight/execution **and** the separately supervised `g4_verify.seal` +
`g4_verify.audit`. The accepted 3/2/8/15-second inner budgets stay unchanged.
There is no second verification window. The deadline is a stop/acceptance gate;
emergency cleanup (existing TERM 1s/KILL 2s/wait 1s) and failure reporting can
finish later. Synchronous kernel/file I/O is not hard real time. In particular,
success requires both `outer-result.json.accepted=true` **and raw adapter exit
0**: a publication that returns past the deadline exits nonzero. A missing exit
or result is unknown/failure, never success.

Each child has a new session, immediate pidfd and PID/start_ticks/PPID/PGID/SID
checks. WNOWAIT reserves its leader until accepted owned/adopted-child cleanup
finishes. The adapter itself sends no signals; the existing helper rechecks
parentage/generation around pidfd signaling. Unknown ownership, remaining
children or cleanup errors fail closed. No numeric PID/PGID fallback exists.
Adapter SIGKILL, kernel hangs, uncontrolled same-UID/root mutation and host-wide
isolation are outside this userspace guarantee.

`outer-guardian.*` and `outer-verification.*` retain raw stdout/stderr.
`outer-result.json` keeps each raw exit, terminating signal, stop, waitid,
identity, cleanup signals/errors/residuals, shared deadline, verifier report,
source/artifact/crypto gates and first failure independently. Exit 0 requires
Guardian success, deadline success, complete cleanup, accepted G4/F1 and N8
record validation, source/policy identity and artifact integrity; all other
attempt outcomes exit 1 (collector/prelaunch exceptions exit 2). Guardian exit
0 alone is never acceptance. Python validates recorded crypto evidence; it does
not perform Groth16 verification. Missing artifacts stay missing. Failures keep
partial files; do not fabricate a successful fixed-set seal. Mutable SHA hashes
provide local consistency, not attestation. Final external archival is separate.

`python3 -B -m unittest -v tools.bench.tests.test_d2_collect` uses pure fixtures,
mocked clocks/processes and static syscall-contract checks only. It forbids real
Popen/socket/signal/ctypes actions. These tests do not establish real-kernel
pidfd/adoption behavior; focused independent review remains a pre-execution gate.
