"""Opt-in cases adapted from the archived runner/barrier controls.

Run only via tests.harness: its independent deadline and subreaper own fallbacks.
No historical test teardown, old buggy runner, Go binary or crypto is imported.
"""
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
import unittest

from tools.bench import processes, resource_audit, run_check, supervise

FIXTURES = Path(__file__).resolve().with_name("fixtures")
GUARDED = os.environ.get("DZKR_BENCH_PROCESS_HARNESS") == str(os.getppid())


@unittest.skipUnless(GUARDED, "use python -B -m tools.bench.tests.harness --out NEW_EXTERNAL_DIR")
class ProcessContracts(unittest.TestCase):
    def setUp(self):
        self.work = Path(os.environ["DZKR_BENCH_CASE_OUT"])
        self.assertTrue(self.work.is_dir())
        self.assertEqual(hashlib.sha256(supervise.RUNNER.read_bytes()).hexdigest(), supervise.RUNNER_SHA)

    def assert_clean(self, result, *, barrier=False):
        # Observe /proc independently of the tool's final cleanup report.
        owners = [result.get("runner_owner"), result.get("target_owner")] if barrier else [result["owner"]]
        observations = []
        for owner in filter(None, owners):
            members = processes.members(owner)
            current = processes.identity(owner["pid"])
            observations.append(dict(owner=owner, current=current, residual_live=members))
            if members or (processes.same(owner, current) and current["state"] not in ("Z", "X")):
                raise RuntimeError("safety failure: owned process still live; stop harness")
        (self.work / "independent-cleanup.json").write_text(json.dumps(observations, indent=2) + "\n")
        self.assertTrue(result["cleanup"]["success"], result)
        self.assertTrue(result["cleanup"]["direct_reaped"])

    def runner(self, mode="exit", code=0, **options):
        result = run_check.run_command(
            [sys.executable, "-B", str(FIXTURES / "child.py"), mode, str(code)],
            self.work / "run", wall_seconds=options.pop("wall_seconds", 1),
            term_grace=.15, kill_grace=.5, **options)
        self.assert_clean(result)
        return result

    def test_runner_success(self):
        result = self.runner()
        self.assertEqual(result["primary"]["outcome"], "command_success")
        self.assertTrue(result["overall_success"])
        self.assertIn("stdout preserved", (self.work / "run/stdout").read_text())
        self.assertIn("stderr preserved", (self.work / "run/stderr").read_text())

    def test_runner_nonzero(self):
        result = self.runner(code=7)
        self.assertEqual(result["primary"]["outcome"], "command_failed")
        self.assertEqual(result["cleanup"]["direct_exit_code"], 7)
        self.assertFalse(result["overall_success"])

    def test_successful_parent_leaves_child(self):
        result = self.runner("orphan")
        self.assertTrue(result["overall_success"])
        self.assertTrue(result["cleanup"]["initial"]["live"])
        self.assertIn("SIGTERM", [s["signal"] for s in result["cleanup"]["signals"]])

    def test_failed_parent_leaves_child(self):
        result = self.runner("orphan", 7)
        self.assertEqual(result["primary"]["outcome"], "command_failed")
        self.assertEqual(result["cleanup"]["direct_exit_code"], 7)
        self.assertTrue(result["cleanup"]["initial"]["live"])

    def test_term_resistant_child(self):
        result = self.runner("resistant")
        self.assertEqual([s["signal"] for s in result["cleanup"]["signals"]], ["SIGTERM", "SIGKILL"])
        self.assertTrue(result["overall_success"])

    def test_timeout(self):
        result = self.runner("hold", wall_seconds=.3)
        self.assertEqual(result["primary"]["outcome"], "timeout")
        self.assertFalse(result["overall_success"])

    def test_monitor_exception_cleanup(self):
        def broken(_pgid):
            # The fixture's pipe proves its one child is ready before faulting.
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if '"event": "READY"' in (self.work / "run/stdout").read_text():
                    raise RuntimeError("controlled monitor failure")
                select.select([], [], [], .005)
            raise RuntimeError("fixture readiness deadline")
        result = self.runner("hold", wall_seconds=3, monitor=broken)
        self.assertEqual(result["primary"]["outcome"], "runner_error")
        self.assertEqual(result["primary"]["message"], "controlled monitor failure")
        self.assertFalse(result["overall_success"])

    def test_unrelated_negative_control(self):
        code = "import sys;print('ready',flush=True);[print(s.strip(),flush=True) for s in sys.stdin]"
        control = subprocess.Popen([sys.executable, "-B", "-u", "-c", code], start_new_session=True,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        owner = processes.identity(control.pid)
        try:
            self.assertTrue(select.select([control.stdout], [], [], 2)[0])
            self.assertEqual(control.stdout.readline(), "ready\n")
            with self.assertRaises(RuntimeError):
                processes.send(dict(owner, start_ticks=owner["start_ticks"] + 1), signal.SIGTERM)
            result = self.runner("hold", wall_seconds=.3)
            self.assertEqual(result["primary"]["outcome"], "timeout")
            self.assertTrue(processes.same(owner, processes.identity(control.pid)))
            control.stdin.write("ping\n")
            control.stdin.flush()
            self.assertTrue(select.select([control.stdout], [], [], 2)[0])
            self.assertEqual(control.stdout.readline(), "ping\n")
            (self.work / "negative-control.json").write_text(json.dumps(dict(owner=owner, ping="ping", alive=True)) + "\n")
        finally:
            # EOF asks only this fresh fixture to exit; outer harness owns failure cleanup.
            control.stdin.close()
            control.wait(timeout=2)
            control.stdout.close()

    def barrier(self, mode="trivial", **options):
        argv = options.pop("argv", [sys.executable, "-B", str(FIXTURES / "peer.py"), mode])
        result = supervise.supervise(argv, self.work / "barrier", role="probe",
                                      barrier_seconds=options.pop("barrier_seconds", 1),
                                      inner_seconds=options.pop("inner_seconds", 2),
                                      outer_seconds=options.pop("outer_seconds", 6), **options)
        self.assert_clean(result, barrier=True)
        return result

    def test_barrier_short_success(self):
        result = self.barrier()
        self.assertTrue(result["success"], result)
        self.assertTrue(result["observation_complete"])
        self.assertLessEqual(result["pre_release_live_sample"]["end_ns"], result["release_begin_ns"])
        self.assertLessEqual(result["release_complete_ns"], result["ack_ns"])
        self.assertIn("PEER_WORK_AFTER_RELEASE", (self.work / "barrier/inner/stdout").read_text())
        audit = resource_audit.audit(self.work / "barrier")
        self.assertIsNone(audit["actual_lifetime_peak_bytes"])
        if not audit["post_release_live_samples"]:
            self.assertIsNone(audit["post_release_sampled_peak_bytes"])

    def test_barrier_broken_ipc(self):
        result = self.barrier("broken")
        self.assertFalse(result["success"])
        self.assertIsNone(result["release_begin_ns"])

    def test_barrier_bad_identity(self):
        result = self.barrier("badpid")
        self.assertFalse(result["success"])
        self.assertIsNone(result["release_begin_ns"])
        self.assertTrue(any("readiness_identity_mismatch" in e.get("message", "") for e in result["errors"]))

    def test_barrier_timeout(self):
        result = self.barrier(observer=lambda _owner: None, barrier_seconds=.3)
        self.assertFalse(result["success"])
        self.assertEqual(result["reason"], "barrier_timeout")
        self.assertIsNone(result["release_begin_ns"])

    def test_barrier_post_release_timeout(self):
        result = self.barrier("hold-fork", inner_seconds=.3)
        self.assertFalse(result["success"])
        self.assertTrue(result["observation_complete"])
        self.assertEqual(result["inner_primary"]["outcome"], "timeout")
        self.assertIn("SIGKILL", [s["signal"] for s in result["inner_cleanup"]["signals"]])
        resource_audit.audit(self.work / "barrier", require_success=False)

    def test_barrier_rejects_exec_wrapper(self):
        if not Path("/usr/bin/env").is_file():
            self.skipTest("/usr/bin/env unavailable")
        result = self.barrier(argv=["/usr/bin/env", sys.executable, "-B", str(FIXTURES / "peer.py"), "trivial"])
        self.assertFalse(result["success"])
        self.assertIsNone(result["release_begin_ns"])
        self.assertNotEqual(result["expected_executable"]["sha256"], result["target_executable"]["sha256"])
