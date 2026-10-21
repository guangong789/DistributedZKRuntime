"""Pure F1 regressions: no main(), subprocesses, signals or target execution."""
import copy
import importlib
import sys
import unittest
from unittest import mock

from tools.bench.tests import harness


class HarnessClassification(unittest.TestCase):
    def setUp(self):
        # Fail without performing an action if a supposedly pure path regresses.
        for name in ("subprocess.Popen", "os.killpg", "signal.pidfd_send_signal", "ctypes.CDLL"):
            patch = mock.patch(name, create=True, side_effect=AssertionError("process action in pure test"))
            patch.start()
            self.addCleanup(patch.stop)

    def executed(self, case="test_runner_success"):
        selector = "tools.bench.tests.process_cases.ProcessContracts." + case
        record = dict(case=case, argv=[sys.executable, "-B", "-m", "unittest", "-v", selector],
                      stop=None, signals=[], peak_descendants=2, minimum_memavailable_bytes=4 * 1024**3,
                      test_owner=dict(pid=123, start_ticks=99, pgid=123, sid=123, state="R", ppid=100),
                      exit_code=0, residual_children=[], elapsed_seconds=.01)
        output = case + " (" + selector + ") ... ok\n\nRan 1 test in 0.001s\n\nOK\n"
        return record, output

    def ineligible(self):
        record, _ = self.executed("test_barrier_rejects_exec_wrapper")
        del record["test_owner"], record["exit_code"]
        record.update(peak_descendants=0, elapsed_seconds=0, skip_reason=harness.ENV_SKIP_REASON)
        return record, None

    def classified(self, record, output):
        result = copy.deepcopy(record)
        status, reason = harness.classify_case(result, output)
        result.update(passed=status == "PASS", skipped=status == "SKIP", classification_reason=reason)
        return result

    def test_executed_assertions_pass(self):
        self.assertEqual(harness.classify_case(*self.executed())[0], "PASS")

    def test_standard_unittest_identity_formats_pass(self):
        record, output = self.executed()
        cls = "tools.bench.tests.process_cases.ProcessContracts"
        full = cls + "." + record["case"]
        case_type = type("ProcessContracts", (unittest.TestCase,),
                         {"__module__": "tools.bench.tests.process_cases",
                          record["case"]: lambda self: None})
        # Constructing/formatting this empty TestCase never runs process_cases.
        current = str(case_type(record["case"])) + " ... ok"
        original = record["case"] + " (" + full + ") ... ok"
        for identity in (record["case"] + " (" + cls + ") ... ok", original, current):
            with self.subTest(identity=identity):
                self.assertEqual(harness.classify_case(record, output.replace(original, identity))[0], "PASS")
        self.assertEqual(harness.classify_case(record, current + "\n" + output)[0], "FAIL")

    def test_pre_execution_ineligibility_is_skip(self):
        record, output = self.ineligible()
        self.assertEqual(harness.classify_case(record, output), ("SKIP", harness.ENV_SKIP_REASON))
        self.assertNotIn("test_owner", record)
        self.assertNotIn("exit_code", record)

    def test_unexpected_unittest_nonzero_fails(self):
        record, output = self.executed()
        record["exit_code"] = 1
        self.assertEqual(harness.classify_case(record, output)[0], "FAIL")

    def test_unexpected_outer_timeout_fails(self):
        record, output = self.executed()
        record["stop"] = "interrupt_or_outer_timeout"
        self.assertEqual(harness.classify_case(record, output)[0], "FAIL")

    def test_unexpected_outer_resource_stop_fails(self):
        record, output = self.executed()
        record["stop"] = "process_count_or_headroom_stop"
        self.assertEqual(harness.classify_case(record, output)[0], "FAIL")

    def test_cleanup_failure_fails(self):
        record, output = self.executed()
        for defect in ("residual", "missing", "signals", "recovered_stop", "error"):
            with self.subTest(defect=defect):
                r = copy.deepcopy(record)
                if defect == "residual":
                    r["residual_children"] = [124]
                elif defect == "missing":
                    del r["residual_children"]
                elif defect == "signals":
                    r["signals"] = [dict(signal="SIGKILL")]
                elif defect == "recovered_stop":
                    r["stop"] = "unexpected_surviving_child"
                else:
                    r["error"] = dict(type="RuntimeError", message="cleanup failed")
                self.assertEqual(harness.classify_case(r, output)[0], "FAIL")

    def test_skip_marker_cannot_hide_failure(self):
        record, _ = self.executed()
        for field, value in (("exit_code", 1), ("stop", "interrupt_or_outer_timeout"),
                             ("stop", "process_count_or_headroom_stop"), ("residual_children", [124])):
            with self.subTest(field=field, value=value):
                r = dict(record, **{field: value})
                status, reason = harness.classify_case(r, "OK (skipped=1)\n")
                self.assertEqual(status, "FAIL")
                self.assertNotIn("after subprocess execution", reason)  # failure takes priority

    def test_skip_marker_after_execution_is_failure(self):
        record, output = self.executed()
        for marker in ("OK (skipped=1)", "case ... skipped 'optional'"):
            with self.subTest(marker=marker):
                self.assertEqual(harness.classify_case(record, marker + "\n" + output)[0], "FAIL")

    def test_missing_output_fails(self):
        record, _ = self.executed()
        for output in (None, "", 123):
            with self.subTest(output=output):
                self.assertEqual(harness.classify_case(record, output)[0], "FAIL")

    def test_malformed_or_incomplete_records_fail(self):
        record, output = self.executed()
        for missing in ("case", "argv", "stop", "signals", "test_owner", "exit_code", "residual_children"):
            with self.subTest(missing=missing):
                r = dict(record)
                del r[missing]
                self.assertEqual(harness.classify_case(r, output)[0], "FAIL")
        for malformed in (None, [], "not a record", dict(record, exit_code=False),
                          dict(record, test_owner=None), dict(record, residual_children=False),
                          dict(record, argv=["unexpected"]), dict(record, test_owner=dict(pid=123))):
            with self.subTest(record=malformed):
                self.assertEqual(harness.classify_case(malformed, output)[0], "FAIL")

    def test_expected_nonzero_target_is_validated_by_test(self):
        # test_runner_nonzero asserts target exit7; its unittest exits0.
        self.assertEqual(harness.classify_case(*self.executed("test_runner_nonzero"))[0], "PASS")

    def test_expected_inner_watchdog_is_validated_by_test(self):
        # Existing cases assert inner timeout; no outer harness stop is expected.
        for case in ("test_timeout", "test_barrier_post_release_timeout"):
            with self.subTest(case=case):
                self.assertEqual(harness.classify_case(*self.executed(case))[0], "PASS")

    def test_expected_inner_watchdog_does_not_excuse_cleanup_failure(self):
        record, output = self.executed("test_timeout")
        record["stop"] = "unexpected_surviving_child"
        self.assertEqual(harness.classify_case(record, output)[0], "FAIL")

    def test_failed_and_skipped_suite_fails(self):
        failed, output = self.executed()
        failed["exit_code"] = 1
        records = [self.classified(failed, output), self.classified(*self.ineligible())]
        self.assertFalse(harness.suite_succeeded(dict(cases=records, residual_children=[]),
                                               [r["case"] for r in records], []))

    def test_all_skipped_is_not_executed_validation(self):
        skipped = self.classified(*self.ineligible())
        self.assertFalse(harness.suite_succeeded(dict(cases=[skipped], residual_children=[]),
                                               [skipped["case"]], []))

    def test_pass_and_eligible_skip_can_complete_suite(self):
        records = [self.classified(*self.executed()), self.classified(*self.ineligible())]
        self.assertTrue(harness.suite_succeeded(dict(cases=records, residual_children=[]),
                                              [r["case"] for r in records], []))

    def test_suite_incomplete_interrupt_cleanup_or_forged_flags_fail(self):
        passed = self.classified(*self.executed())
        summary = dict(cases=[passed], residual_children=[])
        self.assertFalse(harness.suite_succeeded(summary, [passed["case"], "test_timeout"], []))
        self.assertFalse(harness.suite_succeeded(summary, [passed["case"]], [15]))
        self.assertFalse(harness.suite_succeeded(dict(summary, residual_children=[123]), [passed["case"]], []))
        for records in ([], [dict(passed, skipped=True)], [dict(passed, stop="harness_error")],
                        [dict(passed, passed=False, skipped=True)], [dict(passed, passed="yes")], [None]):
            with self.subTest(records=records):
                self.assertFalse(harness.suite_succeeded(dict(cases=records, residual_children=[]),
                                                       [passed["case"]], []))

    def test_unknown_or_empty_skip_reason_fails(self):
        record, _ = self.ineligible()
        for reason in ("", "optional", None):
            with self.subTest(reason=reason):
                self.assertEqual(harness.classify_case(dict(record, skip_reason=reason), None)[0], "FAIL")

    def test_skip_cannot_contradict_execution(self):
        record, _ = self.ineligible()
        for field, value in (("test_owner", None), ("exit_code", 0), ("peak_descendants", 1)):
            with self.subTest(field=field):
                self.assertEqual(harness.classify_case(dict(record, **{field: value}), None)[0], "FAIL")
        self.assertEqual(harness.classify_case(record, "OK (skipped=1)")[0], "FAIL")

    def test_malformed_or_ambiguous_unittest_output_fails(self):
        record, output = self.executed()
        for text in ("OK", output.replace("Ran 1 test", "Ran 0 tests"),
                     output.replace(" ... ok", " ... FAIL"), output + "FAILED\n",
                     output.replace("0.001s", "NaNs"),
                     "Ran 2 tests in 0.01s\n" + output, output + output):
            with self.subTest(output=text):
                self.assertEqual(harness.classify_case(record, text)[0], "FAIL")

    def test_harness_import_has_no_process_side_effect(self):
        importlib.reload(harness)  # setUp blocks launch, signal and subreaper calls.
        self.assertEqual(harness.classify_case(*self.executed())[0], "PASS")


if __name__ == "__main__":
    unittest.main()
