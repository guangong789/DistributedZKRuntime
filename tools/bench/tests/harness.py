"""Serial synthetic-test watchdog, independent of tested runner/supervisor.

Linux pidfds and a process-local subreaper contain only this harness's children.
Never use numeric-PGID fallback, historical binaries, crypto or global limits.
"""
import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import sys
import time

from tools.bench import processes, supervise

CASES = (
    "test_runner_success", "test_runner_nonzero",
    "test_successful_parent_leaves_child", "test_failed_parent_leaves_child",
    "test_term_resistant_child", "test_timeout", "test_monitor_exception_cleanup",
    "test_unrelated_negative_control", "test_barrier_short_success",
    "test_barrier_broken_ipc", "test_barrier_bad_identity", "test_barrier_timeout",
    "test_barrier_post_release_timeout", "test_barrier_rejects_exec_wrapper",
)
ENV_SKIP_REASON = "/usr/bin/env unavailable before execution"


def classify_case(record, output):
    """Classify the unittest subprocess, not its deliberately failing target.

    A known eligibility skip has no subprocess evidence. Once execution starts,
    only a complete single-case unittest success can validate its observations.
    """
    if not isinstance(record, dict) or record.get("case") not in CASES:
        return "FAIL", "missing or malformed case record"
    selector = "tools.bench.tests.process_cases.ProcessContracts." + record["case"]
    if record.get("argv") != [sys.executable, "-B", "-m", "unittest", "-v", selector]:
        return "FAIL", "missing or unexpected test command"
    if "stop" not in record or record["stop"] is not None:
        return "FAIL", "harness stop: " + str(record.get("stop", "missing"))
    if record.get("error") is not None:
        return "FAIL", "harness or evidence error"
    if record.get("residual_children") != [] or record.get("signals") != []:
        return "FAIL", "cleanup incomplete or emergency cleanup required"
    if "skip_reason" in record:
        if (record["case"] != "test_barrier_rejects_exec_wrapper"
                or record["skip_reason"] != ENV_SKIP_REASON):
            return "FAIL", "unknown pre-execution eligibility reason"
        if ("test_owner" in record or "exit_code" in record
                or record.get("peak_descendants") != 0 or output is not None):
            return "FAIL", "skip contradicts execution evidence"
        return "SKIP", record["skip_reason"]
    owner = record.get("test_owner")
    if (not isinstance(owner, dict)
            or any(type(owner.get(k)) is not int or owner[k] <= 1 for k in ("pid", "pgid", "sid"))
            or type(owner.get("start_ticks")) is not int or owner["start_ticks"] < 0):
        return "FAIL", "missing or malformed test process identity"
    if type(record.get("exit_code")) is not int or record["exit_code"] != 0:
        return "FAIL", "missing or unsuccessful unittest exit"
    if not isinstance(output, str):
        return "FAIL", "missing unittest output"
    if "OK (skipped=" in output or "... skipped " in output:
        return "FAIL", "skip reported after subprocess execution started"
    lines = output.strip().splitlines()
    # unittest <=3.10 prints the class; newer versions include the method.
    success = [record["case"] + " (" + identity + ") ... ok"
               for identity in (selector.rsplit(".", 1)[0], selector)]
    counts = [line for line in lines if line.startswith("Ran ")]
    if (sum(lines.count(line) for line in success) != 1 or not lines or lines[-1] != "OK"
            or len(counts) != 1 or re.fullmatch(r"Ran 1 test in [0-9]+(?:\.[0-9]+)?s", counts[0]) is None):
        return "FAIL", "missing or malformed single-case unittest success"
    return "PASS", "executed assertions and unittest validation succeeded"


def suite_succeeded(summary, selected, interrupted):
    """A completed suite needs a real PASS, and no failed required case."""
    if not isinstance(summary, dict) or interrupted or summary.get("residual_children") != []:
        return False
    cases = summary.get("cases")
    if (not isinstance(cases, list) or not all(isinstance(r, dict) for r in cases)
            or [r.get("case") for r in cases] != list(selected)):
        return False
    if not any(r.get("passed") is True for r in cases):
        return False
    for record in cases:
        if ("stop" not in record or record["stop"] is not None or record.get("error") is not None
                or record.get("residual_children") != [] or record.get("signals") != []):
            return False
        if record.get("passed") is True and record.get("skipped") is False:
            continue
        if record.get("passed") is False and record.get("skipped") is True:
            if classify_case(record, None)[0] == "SKIP":
                continue
        return False
    return True


def children(pid):
    try:
        return [int(n) for n in Path(f"/proc/{pid}/task/{pid}/children").read_text().split()]
    except FileNotFoundError:
        return []


def descendants(pid):
    found = set()
    pending = [pid]
    while pending:
        for child in children(pending.pop()):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def reap_adopted(exclude=None):
    for pid in children(os.getpid()):
        if pid == exclude:
            continue
        if os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT):
            os.waitpid(pid, 0)


def signal_children(number, events):
    # Only direct, kernel-parented children. pidfds prevent PID-reuse signaling.
    for pid in children(os.getpid()):
        before = processes.identity(pid)
        if before is None:
            continue
        if before["ppid"] != os.getpid():
            raise RuntimeError("refusing signal: child ownership changed")
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            continue
        try:
            after = processes.identity(pid)
            if not processes.same(before, after) or after["ppid"] != os.getpid():
                raise RuntimeError("refusing signal: child identity changed")
            if after["state"] not in ("Z", "X"):
                signal.pidfd_send_signal(fd, number)
                events.append(dict(signal=signal.Signals(number).name, identity=after))
        except ProcessLookupError:
            pass
        finally:
            os.close(fd)


def cleanup(proc, events):
    # TERM allows the inner runner's own cleanup; KILL is scoped to direct
    # children as descendants become adopted, never the guardian itself.
    for number, seconds in ((signal.SIGTERM, 1), (signal.SIGKILL, 2)):
        deadline = time.monotonic() + seconds
        while children(os.getpid()) and time.monotonic() < deadline:
            signal_children(number, events)
            reap_adopted(proc.pid)
            if os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT):
                # Leave its PID reserved until the explicit Popen wait below.
                if all(pid == proc.pid for pid in children(os.getpid())):
                    break
            select.select([], [], [], .02)
    proc.wait(timeout=1)
    reap_adopted()
    return children(os.getpid())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="new external evidence directory")
    parser.add_argument("--case", choices=("all", *CASES), default="all")
    args = parser.parse_args()
    if sys.platform != "linux" or not all((hasattr(os, "WNOWAIT"), hasattr(os, "pidfd_open"),
                                           hasattr(signal, "pidfd_send_signal"))):
        parser.error("Linux WNOWAIT and pidfd signaling are required; process tests not started")
    if sys.flags.optimize:
        parser.error("normal Python is required by the unchanged historical auditor")
    out = Path(args.out).resolve()
    if out.is_relative_to(Path(supervise.REPO)):
        parser.error("test evidence must be outside the repository")
    available = lambda: next(int(s.split()[1]) * 1024 for s in Path("/proc/meminfo").read_text().splitlines()
                             if s.startswith("MemAvailable:"))
    if available() < 4 * 1024**3:
        parser.error("less than 4GiB available; no process tests started")
    # This setting applies to this fresh CLI process, not the shell or system.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    if children(os.getpid()):
        raise RuntimeError("harness must start without pre-existing children")
    out.mkdir(parents=True, mode=0o700, exist_ok=False)
    interrupted = []
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, lambda n, _f: interrupted.append(n))
    summary = dict(cases=[], passed=False, case_wall_seconds=15, suite_wall_seconds=240,
                   maximum_descendants=8, containment="process-local subreaper + pidfds; no cgroups",
                   crypto_operations=0, skipped=[], residual_children=None)
    suite_start = time.monotonic()
    selected = CASES if args.case == "all" else (args.case,)
    for name in selected:
        if interrupted or time.monotonic() - suite_start > 240:
            break
        work = out / name
        work.mkdir(mode=0o700)
        env = os.environ.copy()
        for key in (*supervise.GATES, "DZKR_MIMCCHAIN_START_BARRIER"):
            env.pop(key, None)
        env.update(PYTHONDONTWRITEBYTECODE="1", DZKR_BENCH_PROCESS_HARNESS=str(os.getpid()),
                   DZKR_BENCH_CASE_OUT=str(work))
        selector = "tools.bench.tests.process_cases.ProcessContracts." + name
        argv = [sys.executable, "-B", "-m", "unittest", "-v", selector]
        record = dict(case=name, argv=argv, stop=None, signals=[], peak_descendants=0,
                      minimum_memavailable_bytes=available())
        if name == "test_barrier_rejects_exec_wrapper" and not Path("/usr/bin/env").is_file():
            record.update(skip_reason=ENV_SKIP_REASON, residual_children=[], elapsed_seconds=0)
            status, reason = classify_case(record, None)
            record.update(passed=status == "PASS", skipped=status == "SKIP", classification_reason=reason)
            summary["cases"].append(record)
            if record["skipped"]:
                summary["skipped"].append(name)
            (work / "harness.json").write_text(json.dumps(record, indent=2) + "\n")
            print(name + (": SKIPPED" if record["skipped"] else ": FAIL"), flush=True)
            if not record["skipped"]:
                break
            continue
        proc = None
        start = time.monotonic()
        try:
            with (work / "stdout").open("xb") as stdout, (work / "stderr").open("xb") as stderr:
                proc = subprocess.Popen(argv, cwd=supervise.REPO, env=env, stdout=stdout,
                                        stderr=stderr, start_new_session=True)
                record["test_owner"] = processes.identity(proc.pid)
                while os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                    reap_adopted(proc.pid)
                    count = len(descendants(os.getpid()))
                    record["peak_descendants"] = max(record["peak_descendants"], count)
                    record["minimum_memavailable_bytes"] = min(record["minimum_memavailable_bytes"], available())
                    if interrupted or time.monotonic() - start > 15 or time.monotonic() - suite_start > 240:
                        record["stop"] = "interrupt_or_outer_timeout"
                        break
                    if count > 8 or record["minimum_memavailable_bytes"] < 2 * 1024**3:
                        record["stop"] = "process_count_or_headroom_stop"
                        break
                    select.select([], [], [], .02)
                if record["stop"]:
                    record["residual_children"] = cleanup(proc, record["signals"])
                else:
                    record["exit_code"] = proc.wait(timeout=1)
                    reap_adopted()
                    record["residual_children"] = children(os.getpid())
                    if record["residual_children"]:
                        record["stop"] = "unexpected_surviving_child"
                        # proc already reaped: clean only remaining direct owned children.
                        for number in (signal.SIGTERM, signal.SIGKILL):
                            signal_children(number, record["signals"])
                            deadline = time.monotonic() + 1
                            while children(os.getpid()) and time.monotonic() < deadline:
                                reap_adopted()
                                select.select([], [], [], .02)
                        record["residual_children"] = children(os.getpid())
        except BaseException as exc:
            record["stop"] = "harness_error"
            record["error"] = dict(type=type(exc).__name__, message=str(exc))
            if proc is not None and proc.returncode is None:
                record["residual_children"] = cleanup(proc, record["signals"])
        record["elapsed_seconds"] = time.monotonic() - start
        try:
            output = (work / "stderr").read_text()
        except (OSError, UnicodeError) as exc:
            output = None
            if record["stop"] is None:
                record["stop"] = "evidence_error"
            record.setdefault("error", dict(type=type(exc).__name__, message=str(exc)))
        status, reason = classify_case(record, output)
        record.update(passed=status == "PASS", skipped=status == "SKIP", classification_reason=reason)
        if record["skipped"]:
            summary["skipped"].append(name)
        summary["cases"].append(record)
        (work / "harness.json").write_text(json.dumps(record, indent=2) + "\n")
        print(name + (": SKIPPED" if record["skipped"] else ": PASS" if record["passed"] else ": FAIL"), flush=True)
        if not record["passed"] and not record["skipped"]:
            break  # No retries and no further process cases after a failure.
    summary["residual_children"] = children(os.getpid())
    summary["passed"] = suite_succeeded(summary, selected, interrupted)
    summary["elapsed_seconds"] = time.monotonic() - suite_start
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    os.umask(0o077)
    sys.exit(main())
