"""No subprocesses, signals or crypto: inherited parsing/audit contracts."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from tools.bench import processes, resource_audit, run_check, supervise


class PureContracts(unittest.TestCase):
    def setUp(self):
        root = os.environ.get("DZKR_BENCH_TEST_OUT")
        if root:
            self.work = Path(tempfile.mkdtemp(prefix="pure-", dir=root))
        else:
            temporary = tempfile.TemporaryDirectory(prefix="dzkr-bench-pure-")
            self.addCleanup(temporary.cleanup)
            self.work = Path(temporary.name)

    def test_source_hashes(self):
        expected = {
            "run_check.py": "b31448c0114902b9f4888077e26f6153453c7f413bf73ad58244a31c3cd6b9ce",
            "processes.py": "148852d2f84c5be3b056e7692487f9795f716197a76f17ee1eb80edfe4ef4230",
            "resource_audit.py": "ef55ca1ba2cf8b16080cd98ff871cfd5462723d3e729bc3037ca076083df67b3",
        }
        for name, digest in expected.items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((supervise.RUNNER.parent / name).read_bytes()).hexdigest(), digest)

    def test_imports_have_no_launch_or_archive_read(self):
        with mock.patch("subprocess.Popen") as launch, mock.patch("os.killpg") as signal_group:
            with mock.patch.object(Path, "read_bytes", side_effect=AssertionError("implicit read")):
                for module in (run_check, processes, supervise, resource_audit):
                    original = vars(module).copy()
                    # Execute import initialization without rebinding shared aliases.
                    spec = importlib.util.spec_from_file_location(module.__name__, module.__file__)
                    isolated = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(isolated)
                    self.assertEqual(vars(module).keys(), original.keys())
                    for name, value in original.items():
                        self.assertIs(vars(module)[name], value, name)
                    if module is supervise:
                        self.assertEqual(isolated.RUNNER, Path(run_check.__file__).resolve())
                        self.assertEqual(Path(isolated.REPO), Path(__file__).resolve().parents[3])
            launch.assert_not_called()
            signal_group.assert_not_called()
        self.assertEqual(supervise.RUNNER, Path(run_check.__file__).resolve())
        self.assertEqual(Path(supervise.REPO), Path(__file__).resolve().parents[3])

    def test_stat_identity_parser(self):
        fields = ["S", "41", "123", "123"] + ["0"] * 15 + ["98765"]
        text = "123 (name with ) brackets) " + " ".join(fields)
        expected = dict(pid=123, state="S", ppid=41, pgid=123, sid=123, start_ticks=98765)
        with mock.patch.object(Path, "read_text", return_value=text):
            self.assertEqual(processes.identity(123), expected)
            self.assertEqual(run_check.identity(123), expected)
        with mock.patch.object(Path, "read_text", side_effect=FileNotFoundError):
            self.assertIsNone(processes.identity(123))
            self.assertIsNone(run_check.identity(123))

    def test_matching_requires_full_identity(self):
        owner = dict(pid=123, start_ticks=99, pgid=123, sid=123)
        for match in (processes.same, run_check.same_process):
            self.assertTrue(match(owner, dict(owner)))
            self.assertFalse(match(owner, None))
            for key in owner:
                with self.subTest(function=match.__name__, key=key):
                    self.assertFalse(match(owner, dict(owner, **{key: owner[key] + 1})))

    def test_group_matching_and_zombie_exclusion(self):
        live = dict(pid=123, state="S", pgid=123, sid=123, start_ticks=1)
        identities = {123: live, 124: dict(live, pid=124, state="Z"),
                      125: dict(live, pid=125, pgid=125, sid=125)}
        entries = []
        for pid in identities:
            entry = mock.MagicMock()
            entry.name = str(pid)
            (entry / "status").read_text.return_value = "Name: probe\nVmRSS: 7 kB\n"
            entries.append(entry)
        with mock.patch.object(run_check, "Path") as path:
            path.return_value.iterdir.return_value = entries
            with mock.patch.object(run_check, "identity", side_effect=identities.get):
                snapshot = run_check.group_snapshot(123)
        self.assertEqual(snapshot["live"], [live])
        self.assertEqual(snapshot["zombies"], [identities[124]])
        self.assertEqual(snapshot["rss_kib"], 7)
        self.assertEqual(snapshot["errors"], [])

    def test_stale_identity_refuses_signal(self):
        owner = dict(pid=123, start_ticks=99, pgid=123, sid=123)
        with mock.patch.object(processes, "identity", return_value=dict(owner, start_ticks=100)):
            with mock.patch("os.killpg") as send:
                with self.assertRaises(RuntimeError):
                    processes.send(owner, 15)
                send.assert_not_called()

    def fixture(self, post=False):
        # A local data file supplies captured executable metadata, never executes.
        executable = self.work / "synthetic-image"
        executable.write_bytes(b"synthetic-only\n")
        st = executable.stat()
        exe = dict(device=st.st_dev, inode=st.st_ino,
                   sha256=hashlib.sha256(executable.read_bytes()).hexdigest())
        owner = dict(pid=123, start_ticks=99, pgid=123, sid=123, state="S")
        pre = dict(start_ns=10, end_ns=20, valid=True, live=True, identity=dict(owner),
                   executable=exe, rss_bytes=4096, system_available_bytes=4 * 1024**3,
                   classification="pre_release")
        rows = [pre]
        if post:
            rows.append(dict(pre, identity=dict(owner), start_ns=50, end_ns=60,
                             rss_bytes=8192, classification="post_release"))
        report = dict(runner_sha256=supervise.RUNNER_SHA, target_argv=[str(executable)],
                      expected_executable=exe, target_owner=dict(owner),
                      peer=dict(pid=123, uid=os.getuid()), observation_complete=True,
                      release_begin_ns=30, release_complete_ns=35, ack_ns=40,
                      pre_release_live_sample=pre, readiness_rss_bytes=4096,
                      all_live_sampled_peak_bytes=8192 if post else 4096,
                      post_release_live_samples=int(post),
                      post_release_sampled_peak_bytes=8192 if post else None,
                      actual_lifetime_peak_bytes=None,
                      post_release_coverage="sampled_not_complete_peak" if post else "unobserved",
                      success=True, reason=None, errors=[],
                      cleanup=dict(success=True, direct_reaped=True, residual_live=[]),
                      inner_primary=dict(outcome="command_success"),
                      inner_cleanup=dict(success=True, direct_exit_code=0),
                      events=[dict(event=name, monotonic_ns=i) for i, name in enumerate(
                          ["target_identified", "sampler_ready", "before_release", "released", "target_acknowledged"])])
        return report, rows

    def audit(self, report, rows):
        (self.work / "supervisor.json").write_text(json.dumps(report))
        (self.work / "resources.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
        return resource_audit.audit(self.work)

    def test_readiness_is_not_work_peak(self):
        result = self.audit(*self.fixture())
        self.assertEqual(result["resource_classification"], "readiness_only")
        self.assertIsNone(result["post_release_sampled_peak_bytes"])
        self.assertIsNone(result["actual_lifetime_peak_bytes"])

    def test_post_peak_recomputed(self):
        result = self.audit(*self.fixture(post=True))
        self.assertEqual(result["post_release_sampled_peak_bytes"], 8192)
        self.assertEqual(result["post_release_live_samples"], 1)

    def test_false_evidence_rejected(self):
        report, rows = self.fixture(post=True)
        for defect in ("peak", "pid", "ticks", "zombie", "early_release", "cleanup", "missing"):
            with self.subTest(defect=defect):
                r, samples = copy.deepcopy((report, rows))
                if defect == "peak":
                    r["all_live_sampled_peak_bytes"] += 1
                elif defect in ("pid", "ticks"):
                    samples[-1]["identity"]["pid" if defect == "pid" else "start_ticks"] += 1
                elif defect == "zombie":
                    samples[-1]["identity"]["state"] = "Z"
                elif defect == "early_release":
                    r["release_begin_ns"] = 5
                elif defect == "cleanup":
                    r["cleanup"]["success"] = False
                else:
                    samples.clear()
                with self.assertRaises(AssertionError):
                    self.audit(r, samples)

    def test_missing_and_malformed_records(self):
        with self.assertRaises(FileNotFoundError):
            resource_audit.audit(self.work)
        (self.work / "supervisor.json").write_text("{")
        with self.assertRaises(ValueError):
            resource_audit.audit(self.work)


if __name__ == "__main__":
    unittest.main()
