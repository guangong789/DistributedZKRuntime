"""Linux-only bounded command runner; no containment of deliberate setsid/setpgid escapes.

No Popen.poll() before cleanup: waitid(WNOWAIT) keeps the session leader's PID
reserved even after exit. Signals require its original /proc starttime, PGID and
SID. SIGKILL of this runner, kernel failure and blocking filesystem I/O cannot be
handled by Python finally; use an independent outer supervisor for experiments.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

OVERRIDES = dict(GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", GOMAXPROCS="4")
FLAGS = ("DZKR_MIMCCHAIN_CALIBRATE", "DZKR_MIMCCHAIN_GROTH16")
PHASE_BUDGETS = dict(compile=30, setup=90, setup_wrong_vk=90, prove=60)
RSS_STOP_KIB = 2 * 1024 * 1024
AVAILABLE_STOP_KIB = 2 * 1024 * 1024
ESCALATION_STOP_KIB = 1024 * 1024


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def identity(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return None
    fields = raw[raw.rfind(")") + 2:].split()
    return dict(pid=pid, state=fields[0], ppid=int(fields[1]), pgid=int(fields[2]),
                sid=int(fields[3]), start_ticks=int(fields[19]))


def same_process(a, b):
    return a is not None and b is not None and all(a[k] == b[k] for k in ("pid", "start_ticks", "pgid", "sid"))


def group_snapshot(pgid):
    live, zombies, errors, rss = [], [], [], 0
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            item = identity(int(entry.name))
            if item is None or item["pgid"] != pgid:
                continue
            if item["state"] in ("Z", "X"):
                zombies.append(item)
                continue
            status = (entry / "status").read_text().splitlines()
            current = identity(item["pid"])
            if not same_process(item, current):
                continue
            live.append(current)
            rss += sum(int(x.split()[1]) for x in status if x.startswith("VmRSS:"))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (PermissionError, OSError, ValueError) as exc:
            errors.append(dict(pid=int(entry.name), error_type=type(exc).__name__))
    return dict(live=live, zombies=zombies, errors=errors, rss_kib=rss)


def sample(pgid):
    group = group_snapshot(pgid)
    if group["errors"]:
        raise RuntimeError("process-group observation incomplete")
    available = next(int(x.split()[1]) for x in Path("/proc/meminfo").read_text().splitlines()
                     if x.startswith("MemAvailable:"))
    return group, available


class Interrupts:
    """Signal handler records intent; repeated signals cannot interrupt finally."""
    def __init__(self):
        self.signals = []
        self.previous = {}

    def __enter__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.previous[sig] = signal.getsignal(sig)
            signal.signal(sig, lambda number, frame: self.signals.append(number))
        return self

    def __exit__(self, *args):
        for sig, handler in self.previous.items():
            signal.signal(sig, handler)


def observed_exit(process):
    # Does not reap: the original PID/session anchor is retained through cleanup.
    return os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)


def cleanup(process, owner, term_grace, kill_grace):
    result = dict(started_utc=utc(), outcome="unverifiable", signals=[], errors=[],
                  direct_reaped=False, direct_exit_code=process.returncode)
    start = time.monotonic()
    snapshots = []

    def inspect():
        found = group_snapshot(process.pid)
        snapshots.append(found)
        return found

    def signal_group(number):
        # Never authorize signaling using only a cached numeric PGID.
        current = identity(process.pid)
        if owner is None or not same_process(owner, current):
            raise RuntimeError("original session leader identity no longer verifiable")
        if owner["pid"] <= 1 or owner["pgid"] != owner["pid"] or owner["sid"] != owner["pid"] or owner["pgid"] == os.getpgrp():
            raise RuntimeError("unsafe process-group identity")
        if any(p["sid"] != owner["sid"] for p in inspect()["live"]):
            raise RuntimeError("unexpected session in owned process group")
        try:
            os.killpg(owner["pgid"], number)
            result["signals"].append(dict(signal=signal.Signals(number).name, utc=utc()))
        except ProcessLookupError:
            result["signals"].append(dict(signal=signal.Signals(number).name, utc=utc(), vanished=True))

    def wait_group(seconds):
        deadline = time.monotonic() + seconds
        while True:
            state = inspect()
            if not state["live"] or time.monotonic() >= deadline:
                return state
            time.sleep(min(.02, max(0, deadline - time.monotonic())))

    try:
        state = inspect()
        result["initial"] = state
        if not state["live"]:
            result["outcome"] = "not_needed"
        else:
            signal_group(signal.SIGTERM)
            state = wait_group(term_grace)
            result["outcome"] = "after_sigterm"
            if state["live"]:
                signal_group(signal.SIGKILL)
                state = wait_group(kill_grace)
                result["outcome"] = "after_sigkill" if not state["live"] else "incomplete"
        if any(s["errors"] for s in snapshots):
            result["outcome"] = "unverifiable"
    except BaseException as exc:
        result["errors"].append(dict(error_type=type(exc).__name__, message=str(exc)))
        result["outcome"] = "unverifiable"
    finally:
        try:
            # If cleanup could not safely signal, don't perform an unbounded wait.
            process.wait(timeout=kill_grace)
            result["direct_reaped"] = True
            result["direct_exit_code"] = process.returncode
        except BaseException as exc:
            result["errors"].append(dict(stage="reap", error_type=type(exc).__name__, message=str(exc)))
            result["outcome"] = "incomplete"
        try:
            result["final"] = inspect()
            if result["final"]["errors"]:
                result["outcome"] = "unverifiable"
            elif result["final"]["live"]:
                result["outcome"] = "incomplete"
        except BaseException as exc:
            result["errors"].append(dict(stage="final_inspection", error_type=type(exc).__name__, message=str(exc)))
            result["outcome"] = "unverifiable"
        result["success"] = result["outcome"] in ("not_needed", "after_sigterm", "after_sigkill") and result["direct_reaped"]
        result["elapsed_seconds"] = time.monotonic() - start
        result["ended_utc"] = utc()
    return result


def write_json(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def child_limits():
    # Necessary resource-only pre-exec hook: targets and even their earliest forks
    # inherit the original limits. No setsid/setpgid or other process manipulation.
    # This standalone runner must run single-threaded (preexec_fn constraint).
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CPU, (180, 190))


def run_command(argv, outdir, *, label="command", cwd=None, wall_seconds=210,
                term_grace=1, kill_grace=2, monitor=sample, reporter=write_json):
    """monitor/reporter are narrow callable seams for lifecycle fault probes."""
    if sys.platform != "linux" or not hasattr(os, "WNOWAIT"):
        raise RuntimeError("Linux /proc and waitid(WNOWAIT) required")
    if not argv or any(not math.isfinite(x) or x <= 0 for x in (wall_seconds, term_grace, kill_grace)):
        raise ValueError("command and finite positive lifecycle budgets required")
    outdir = Path(outdir)
    outdir.mkdir(mode=0o700, parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(OVERRIDES)
    for flag in FLAGS:
        env.pop(flag, None)
    if label in ("calibration", "groth16"):
        env[FLAGS[0 if label == "calibration" else 1]] = "1"
    record = dict(argv=list(argv), cwd=os.path.abspath(cwd or os.getcwd()), started_utc=utc(),
                  environment=OVERRIDES, opt_in_flags={k:env[k] for k in FLAGS if k in env},
                  limits=dict(wall_seconds=wall_seconds, phase_seconds=PHASE_BUDGETS,
                              rss_stop_kib=RSS_STOP_KIB, available_stop_kib=AVAILABLE_STOP_KIB,
                              stop_escalation_kib=ESCALATION_STOP_KIB, cpu_seconds=[180,190],
                              term_grace=term_grace, kill_grace=kill_grace, kernel_memory_limit=False),
                  primary=dict(outcome="not_started"), evidence_errors=[], evidence_complete=True,
                  stage_observations=[], sampled_group_peak_rss_kib=0, minimum_memavailable_kib=None)
    process = owner = None
    start = time.monotonic()
    stdout_path, stderr_path = outdir / "stdout", outdir / "stderr"
    with Interrupts() as interrupts:
        try:
            write_json(outdir / "started.json", record)
            with stdout_path.open("xb") as out, stderr_path.open("xb") as err:
                process = subprocess.Popen(argv, cwd=record["cwd"], env=env, stdout=out, stderr=err,
                                           start_new_session=True, preexec_fn=child_limits)
                owner = identity(process.pid)
                record["owner"] = owner
                if owner is None or owner["sid"] != process.pid or owner["pgid"] != process.pid:
                    raise RuntimeError("unable to establish owned session")
                write_json(outdir / "owner.json", owner)
                offset, partial, phase, phase_start = 0, "", None, None
                while True:
                    if interrupts.signals:
                        record["primary"] = dict(outcome="interrupted", signal=interrupts.signals[0])
                        break
                    event = observed_exit(process)
                    if event is not None:
                        record["primary"] = dict(outcome="command_success" if event.si_code == os.CLD_EXITED and event.si_status == 0 else "command_failed",
                                                 waitid_code=event.si_code, waitid_status=event.si_status)
                        break
                    now = time.monotonic()
                    if now - start > wall_seconds:
                        record["primary"] = dict(outcome="timeout", reason="overall wall watchdog")
                        break
                    state, available = monitor(process.pid)
                    record["sampled_group_peak_rss_kib"] = max(record["sampled_group_peak_rss_kib"], state["rss_kib"])
                    record["minimum_memavailable_kib"] = available if record["minimum_memavailable_kib"] is None else min(available, record["minimum_memavailable_kib"])
                    with stdout_path.open(errors="replace") as stream:
                        stream.seek(offset); partial += stream.read(); offset = stream.tell()
                    lines = partial.split("\n"); partial = lines.pop()
                    for line in lines:
                        if "MIMCCHAIN_STAGE " not in line:
                            continue
                        marker = json.loads(line.split("MIMCCHAIN_STAGE ",1)[1])
                        record["stage_observations"].append(marker)
                        if marker["event"] == "start":
                            phase, phase_start = marker["stage"], now
                        else:
                            phase = phase_start = None
                    if phase in PHASE_BUDGETS and now - phase_start > PHASE_BUDGETS[phase]:
                        record["primary"] = dict(outcome="timeout", reason="phase watchdog: " + phase)
                        break
                    if state["rss_kib"] > RSS_STOP_KIB or available < AVAILABLE_STOP_KIB:
                        record["primary"] = dict(outcome="resource_stop", reason="group RSS" if state["rss_kib"] > RSS_STOP_KIB else "system MemAvailable")
                        break
                    time.sleep(.05)
        except BaseException as exc:
            failure = dict(outcome="runner_error", error_type=type(exc).__name__, message=str(exc))
            if record["primary"]["outcome"] == "not_started":
                record["primary"] = failure
            else:
                record.setdefault("execution_errors", []).append(failure)
                record["evidence_complete"] = False
        finally:
            if process is not None:
                record["cleanup"] = cleanup(process, owner, term_grace, kill_grace)
            else:
                record["cleanup"] = dict(outcome="not_needed", success=True, direct_reaped=None)
            # Signals arriving during cleanup must not turn a normal run into success.
            record["runner_signals"] = interrupts.signals
            if interrupts.signals and record["primary"]["outcome"] == "command_success":
                record["primary"] = dict(outcome="interrupted", signal=interrupts.signals[0])
            record["elapsed_seconds"] = time.monotonic() - start
            record["ended_utc"] = utc()
        record["stop_escalation"] = record["sampled_group_peak_rss_kib"] > ESCALATION_STOP_KIB
        record["child_peak_rss_kib"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        record["raw"] = {}
        for path in (stdout_path, stderr_path):
            try:
                record["raw"][path.name] = dict(bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            except BaseException as exc:
                record["evidence_errors"].append(dict(stage="raw_capture", error_type=type(exc).__name__, message=str(exc)))
                record["evidence_complete"] = False
        record["overall_success"] = record["primary"]["outcome"] == "command_success" and record["cleanup"]["success"] and record["evidence_complete"] and not record["stop_escalation"] and not interrupts.signals
        try:
            reporter(outdir / "result.json", record)
        except BaseException as exc:
            record["evidence_errors"].append(dict(stage="report", error_type=type(exc).__name__, message=str(exc)))
            record["evidence_complete"] = record["overall_success"] = False
            try:
                write_json(outdir / "report-failure.json", record)
            except BaseException:
                print("runner: evidence output failed after cleanup", file=sys.stderr)
        return record


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--label", default="command")
    parser.add_argument("--cwd")
    parser.add_argument("--wall-seconds", type=float, default=210)
    parser.add_argument("--term-grace", type=float, default=1)
    parser.add_argument("--kill-grace", type=float, default=2)
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
    record = run_command(argv, args.out, label=args.label, cwd=args.cwd, wall_seconds=args.wall_seconds,
                         term_grace=args.term_grace, kill_grace=args.kill_grace)
    print(json.dumps({k:record[k] for k in ("primary", "cleanup", "overall_success")}), flush=True)
    return 0 if record["overall_success"] else 1


if __name__ == "__main__":
    sys.exit(main())
