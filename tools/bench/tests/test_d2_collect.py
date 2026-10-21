"""Pure/mock D2 contracts. Guards forbid real targets, sockets and signaling."""
import ast
import copy
import os
from pathlib import Path
import signal
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

from tools.bench import d2_collect as d, policy_binding as b
from tools.bench.tests import test_g4_verify as fixtures, test_g4_crypto as crypto_fixtures


class Collector(unittest.TestCase):
    def setUp(self):
        self.helper = fixtures.G4FinalEvidence('test_valid_complete_bundle')
        self.helper._testMethodName = 'd2-' + self._testMethodName
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.out = self.helper.root/'SYNTHETIC-NOT-A-REAL-D2-ATTEMPT'
        self.out.mkdir()
        self.config = self.out/'g4.json'
        self.m = dict(paths=dict(out=str(self.out), channel=str(self.out/'unused-ipc/socket')),
                      cwd=str(b.REPO), python=dict(path='/synthetic/python'), environment={}, run_id='a'*32)
        self.intent = dict(outer_seconds=25, config_sha256='b'*64, environment={},
                           adapter_argv=['SYNTHETIC-NOT-EXECUTED'],
                           commands=d.commands(dict(self.m, paths=dict(self.m['paths'], config=str(self.config)))))
        self.now = 100
        self.rows = {p: dict(argv=a, raw_exit_code=0, signal=None, stop=None,
                            exited_before_deadline=True, cleanup_complete=True, residual_children=[], errors=[])
                     for p, a in self.intent['commands'].items()}
        self.audit = dict(run_id=self.m['run_id'], final_accepted=True, evidence_valid=True,
                          crypto=dict(evidence_valid=True), errors=[])
        self.effect = None

    def phase(self, argv, phase, m, deadline, pending):
        self.assertEqual(argv, self.intent['commands'][phase])
        self.assertEqual(deadline, 25_000_000_100)  # No new window for verification.
        if phase == 'guardian':
            self.helper.put(self.out, 'guardian.json', dict(stop=None))
            self.helper.put(self.out, 'g4-artifacts.json', {})
            (self.out/'crypto').mkdir()
            (self.out/'crypto/ledger.jsonl').write_bytes(b'SYNTHETIC-NOT-CRYPTO\n')
        else:
            self.helper.put(self.out, 'outer-verification.stdout', self.audit)
        if self.effect:
            self.effect(phase, pending)
        return copy.deepcopy(self.rows[phase])

    def run_model(self, checked=None, *, late_interrupt=False, expire_on_restore=False,
                  via_cli=False, publication=None):
        active = {}
        self.handler_restores = []
        original_write = b.write_private

        def signal_step(number, handler):
            previous = active.get(number, signal.SIG_DFL)
            if callable(previous):
                self.handler_restores.append(number)
                if number == signal.SIGINT:  # Last owned handler, still installed here.
                    if late_interrupt:
                        previous(number, None)
                    if expire_on_restore:
                        self.now = 25_000_000_100
            active[number] = handler
            return previous

        def write(path, raw):
            if Path(path).name == 'outer-result.json' and publication:
                publication(dict(active), b.decode(raw))
            original_write(path, raw)

        with mock.patch.object(d, 'checked', side_effect=checked, return_value=(self.m, self.intent)), \
                mock.patch.object(d.binding, 'executable', return_value=self.m['python']), \
                mock.patch.object(sys, 'orig_argv', self.intent['adapter_argv']), \
                mock.patch.dict(os.environ, self.intent['environment'], clear=True), \
                mock.patch.object(d, 'lock', return_value=987), mock.patch.object(os, 'close'), \
                mock.patch.object(d, 'preflight', return_value={'SYNTHETIC': True}), \
                mock.patch.object(d.ctypes, 'CDLL') as libc, \
                mock.patch.object(d.signal, 'signal', side_effect=signal_step), \
                mock.patch.object(d.ps, 'identity', return_value={}), \
                mock.patch.object(b, 'write_private', side_effect=write), \
                mock.patch.object(d.harness, 'children', return_value=[]), \
                mock.patch.object(d.time, 'monotonic_ns', side_effect=lambda: self.now), \
                mock.patch.object(d, 'child', side_effect=self.phase) as child:
            libc.return_value.prctl.return_value = 0
            if via_cli:
                with mock.patch.object(sys, 'argv', ['d2_collect', 'run', '--config', str(self.config)]):
                    self.cli_exit = d.main()
                result = b.decode((self.out/'outer-result.json').read_bytes())
            else:
                result = d.collect(self.config)
        self.assertEqual(b.decode((self.out/'outer-result.json').read_bytes()), result)
        return result, child

    def test_late_interrupt_after_preliminary_acceptance_rejects_publication_and_cli(self):
        r, calls = self.run_model(late_interrupt=True, via_cli=True)
        self.assertEqual(self.handler_restores, [signal.SIGTERM, signal.SIGINT])
        self.assertEqual(calls.call_count, 2)
        self.assertEqual(r['interrupts'], [signal.SIGINT])
        self.assertFalse(r['outer_timed_out'])
        self.assertTrue(all(r[k] for k in ('cleanup_complete', 'g4_evidence_accepted',
                                          'crypto_evidence_accepted', 'source_binding_valid',
                                          'artifact_integrity_valid')))
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], 0)
        with self.subTest(decision='serialized acceptance'):
            self.assertFalse(r['accepted'], 'late handled interrupt must veto preliminary success')
        with self.subTest(decision='CLI exit'):
            self.assertNotEqual(self.cli_exit, 0, 'recorded interruption must not return CLI zero')
        with self.subTest(decision='failure provenance'):
            self.assertEqual(r['primary_failure'], 'collector_interrupted')

    def test_final_commit_preserves_uninterrupted_success(self):
        r, _ = self.run_model(via_cli=True)
        self.assertEqual(r['interrupts'], [])
        self.assertTrue(r['accepted'])
        self.assertEqual(self.cli_exit, 0)
        self.assertIsNone(r['primary_failure'])
        self.assertEqual(r['verification_report'], self.audit)

    def test_final_commit_timeout_rejects_preliminary_success(self):
        r, _ = self.run_model(expire_on_restore=True, via_cli=True)
        self.assertEqual(r['interrupts'], [])
        self.assertTrue(r['outer_timed_out'])
        self.assertFalse(r['accepted'])
        self.assertNotEqual(self.cli_exit, 0)

    def test_final_commit_timeout_and_interrupt_both_prevent_success(self):
        r, _ = self.run_model(late_interrupt=True, expire_on_restore=True, via_cli=True)
        self.assertEqual(r['interrupts'], [signal.SIGINT])
        self.assertTrue(r['outer_timed_out'])
        self.assertFalse(r['accepted'])
        self.assertNotEqual(self.cli_exit, 0)

    def test_late_interrupt_preserves_guardian_failure(self):
        self.rows['guardian']['raw_exit_code'] = 7
        r, _ = self.run_model(late_interrupt=True, via_cli=True)
        self.assertFalse(r['accepted'])
        self.assertNotEqual(self.cli_exit, 0)
        self.assertEqual(r['primary_failure'], 'child_nonzero')
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], 7)
        self.assertEqual(r['interrupts'], [signal.SIGINT])
        self.assertEqual(r['verification_report'], self.audit)

    def test_late_interrupt_preserves_incomplete_cleanup(self):
        self.rows['guardian'].update(cleanup_complete=False, residual_children=[123])
        r, calls = self.run_model(late_interrupt=True, via_cli=True)
        self.assertEqual(calls.call_count, 1)
        self.assertFalse(r['accepted'] or r['cleanup_complete'])
        self.assertNotEqual(self.cli_exit, 0)
        self.assertEqual(r['primary_failure'], 'cleanup_incomplete')
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], 0)
        self.assertEqual(r['phases']['guardian']['residual_children'], [123])
        self.assertEqual(r['interrupts'], [signal.SIGINT])

    def test_publication_occurs_after_owned_handlers_retire(self):
        snapshots = []
        def published(active, record):
            self.assertEqual(active, {signal.SIGTERM: signal.SIG_DFL, signal.SIGINT: signal.SIG_DFL})
            self.assertEqual(record['interrupts'], [signal.SIGINT])
            self.assertFalse(record['accepted'])
            snapshots.append(record)
        r, _ = self.run_model(late_interrupt=True, via_cli=True, publication=published)
        self.assertEqual(snapshots, [r])
        self.assertNotEqual(self.cli_exit, 0)

    def test_cli_rejects_interrupted_or_unknown_final_state(self):
        for value in ([signal.SIGINT], None, 'unknown'):
            with self.subTest(interrupts=value), \
                    mock.patch.object(sys, 'argv', ['d2_collect', 'run', '--config', str(self.config)]), \
                    mock.patch.object(d, 'collect', return_value=dict(accepted=True, interrupts=value, deadline_ns=2)), \
                    mock.patch.object(d.time, 'monotonic_ns', return_value=1):
                self.assertNotEqual(d.main(), 0)

    def test_complete_mock_success_is_not_a_real_experiment(self):
        r, calls = self.run_model()
        self.assertTrue(r['accepted'])
        self.assertEqual(calls.call_count, 2)
        self.assertTrue(r['source_binding_valid'] and r['artifact_integrity_valid'])
        self.assertTrue(r['cleanup_complete'] and r['crypto_evidence_accepted'])
        self.assertEqual(r['verification_report'], self.audit)

    def test_guardian_nonzero_preserves_raw_exit_and_first_failure(self):
        self.rows['guardian'].update(raw_exit_code=-9, signal=9)
        self.rows['verification'].update(raw_exit_code=2)
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['primary_failure'], 'child_nonzero')
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], -9)
        self.assertEqual(r['phases']['guardian']['signal'], 9)
        self.assertEqual(r['phases']['verification']['raw_exit_code'], 2)

    def test_zero_exit_rejected_evidence_and_crypto_failure(self):
        self.audit.update(final_accepted=False, evidence_valid=False, errors=[{'message': 'crypto artifact identity'}])
        self.audit['crypto']['evidence_valid'] = False
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        labels = {x['reason'] for x in r['failures']}
        self.assertTrue({'artifact_corruption', 'crypto_evidence_rejected', 'evidence_rejected'} <= labels)

    def test_missing_evidence_is_not_fabricated(self):
        def missing(phase, _):
            if phase == 'verification':
                for n in ('guardian.json', 'g4-artifacts.json', 'crypto/ledger.jsonl'):
                    (self.out/n).unlink()
        self.effect = missing
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertTrue({'missing_guardian_result', 'missing_g4_evidence', 'missing_crypto_ledger'}
                        <= {x['reason'] for x in r['failures']})

    def test_guardian_hang_outer_timeout_does_not_run_verifier(self):
        self.rows['guardian'].update(stop='outer_timeout', raw_exit_code=-15, signal=15,
                                      exited_before_deadline=False)
        r, calls = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertTrue(r['outer_timed_out'])
        self.assertEqual(calls.call_count, 1)

    def test_guardian_unexpected_exit_is_unsuccessful(self):
        self.rows['guardian'].update(stop='collector_exception', raw_exit_code=1)
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['primary_failure'], 'collector_exception')

    def test_guardian_exits_before_descendants(self):
        self.rows['guardian']['stop'] = 'unexpected_descendants'
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['primary_failure'], 'unexpected_descendants')

    def test_incomplete_cleanup_and_unknown_identity_stop_further_launches(self):
        self.rows['guardian'].update(cleanup_complete=False, residual_children=[123],
                                      errors=['cleanup:RuntimeError'])
        r, calls = self.run_model()
        self.assertFalse(r['accepted'] or r['cleanup_complete'])
        self.assertEqual(calls.call_count, 1)
        self.assertEqual(r['primary_failure'], 'cleanup_incomplete')

    def test_collector_exception_has_independent_error(self):
        def broken(phase, _):
            raise OSError('synthetic collector failure')
        self.effect = broken
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['collector_errors'], ['OSError'])

    def test_cleanup_error_cannot_mask_original_child_failure(self):
        self.rows['guardian'].update(raw_exit_code=7, cleanup_complete=False,
                                      errors=['cleanup:OSError'])
        r, calls = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(calls.call_count, 1)
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], 7)
        self.assertEqual(r['phases']['guardian']['errors'], ['cleanup:OSError'])

    def test_emergency_cleanup_does_not_create_synthetic_success(self):
        self.rows['guardian']['signals'] = [{'signal': 'SIGTERM', 'identity': {'SYNTHETIC': True}}]
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['primary_failure'], 'emergency_cleanup')

    def test_source_identity_changed_after_launch(self):
        count = 0
        def check(_):
            nonlocal count
            count += 1
            if count == 4:
                raise ValueError('source binding')
            return self.m, self.intent
        r, _ = self.run_model(check)
        self.assertFalse(r['accepted'] or r['source_binding_valid'])
        self.assertEqual(r['collector_errors'], ['ValueError'])

    def test_wrong_verification_run_id_rejected(self):
        self.audit['run_id'] = 'c'*32
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['collector_errors'], ['PolicyError'])
        self.assertIn('verification_run_id_mismatch', {x['reason'] for x in r['failures']})

    def test_resource_stop_despite_zero_exit_rejected(self):
        def resource(phase, _):
            if phase == 'guardian':
                self.helper.put(self.out, 'guardian.json', dict(stop='process_count_or_headroom_stop',
                                                               reason='resource_stop'))
        self.effect = resource
        self.audit.update(final_accepted=False)
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertIn('resource_stop', {x['reason'] for x in r['failures']})

    def test_postverification_timeout_and_late_success_are_sticky(self):
        def late(phase, _):
            if phase == 'verification':
                self.now = 25_000_000_100
        self.effect = late
        r, calls = self.run_model()
        self.assertEqual(calls.call_count, 2)
        self.assertTrue(r['outer_timed_out'])
        self.assertFalse(r['accepted'])

    def test_postverification_hang_has_same_deadline_and_raw_signal(self):
        self.rows['verification'].update(stop='outer_timeout', raw_exit_code=-15, signal=15,
                                          exited_before_deadline=False)
        r, calls = self.run_model()
        self.assertEqual(calls.call_count, 2)
        self.assertTrue(r['outer_timed_out'])
        self.assertFalse(r['accepted'])
        self.assertEqual(r['phases']['guardian']['raw_exit_code'], 0)
        self.assertEqual(r['phases']['verification']['signal'], 15)

    def test_barrier_failure_diagnostic_preserves_validator_rejection(self):
        def barrier(phase, _):
            if phase == 'guardian':
                self.helper.put(self.out, 'supervised/supervisor.json', dict(reason='barrier_timeout'))
        self.effect = barrier
        self.audit.update(final_accepted=False)
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertIn('barrier_failure', {x['reason'] for x in r['failures']})

    def test_parent_signal_cannot_become_success(self):
        self.effect = lambda phase, pending: pending.append(signal.SIGTERM)
        r, calls = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(calls.call_count, 1)
        self.assertEqual(r['primary_failure'], 'collector_interrupted')
        self.assertEqual(r['interrupts'], [signal.SIGTERM])

    def test_exclusive_marker_blocks_second_launch(self):
        self.run_model()
        with self.assertRaisesRegex(ValueError, 'already consumed'):
            self.run_model()

    def test_preflight_failure_does_not_consume_attempt(self):
        with mock.patch.object(d, 'checked', return_value=(self.m, self.intent)), \
                mock.patch.object(d.binding, 'executable', return_value=self.m['python']), \
                mock.patch.object(sys, 'orig_argv', self.intent['adapter_argv']), \
                mock.patch.dict(os.environ, self.intent['environment'], clear=True), \
                mock.patch.object(d, 'lock', return_value=987), mock.patch.object(os, 'close'), \
                mock.patch.object(d, 'preflight', side_effect=ValueError('eligibility')), \
                mock.patch.object(d, 'child') as child:
            with self.assertRaisesRegex(ValueError, 'eligibility'):
                d.collect(self.config)
        child.assert_not_called()
        self.assertFalse((self.out/'outer-attempt.json').exists())

    def test_cli_propagates_unsuccessful_and_late_publication(self):
        for accepted, now, expected in ((False, 1, 1), (True, 3, 1), (True, 1, 0)):
            with self.subTest(accepted=accepted, now=now), \
                    mock.patch.object(sys, 'argv', ['d2_collect', 'run', '--config', str(self.config)]), \
                    mock.patch.object(d, 'collect', return_value=dict(accepted=accepted, interrupts=[], deadline_ns=2)), \
                    mock.patch.object(d.time, 'monotonic_ns', return_value=now):
                self.assertEqual(d.main(), expected)

    def test_profile_and_source_snapshots_use_real_binding_without_execution(self):
        h = crypto_fixtures.CryptoProfile('test_explicit_profile_and_bound_sources')
        h._testMethodName = 'd2-actual-contract'
        h.setUp()
        self.addCleanup(h.doCleanups)
        label = 'n8-cpu-pilot-20990101T000000Z/g4'
        (h.root/Path(label).parent).mkdir()
        config = h.prepare(label)
        with mock.patch.object(d, 'D2_PARENT', h.root):
            intent = d.prepare(config)
            self.assertEqual(d.checked(config)[1], intent)
            self.assertEqual(intent['outer_seconds'], 25)
            self.assertEqual(set(intent['sources']), set(d.EXTRA))
            self.assertEqual(intent['commands']['guardian'][1:5], ['-B', '-m', 'tools.bench.g4_launch', '--config'])
            self.assertFalse((config.parent/'outer-attempt.json').exists())
            snapshot = config.parent/'outer-sources/tests/harness.py'
            snapshot.unlink()
            snapshot.write_bytes(b'wrong source')
            with self.assertRaisesRegex(ValueError, 'source snapshot'):
                d.checked(config)
            original = b.decode(config.read_bytes())
            original['crypto']['profile'] = 'unknown'
            self.helper.put(config.parent, 'g4.json', original)
            with self.assertRaises(ValueError):
                d.contract(config)

    def test_stale_directory_and_consumed_markers_fail_before_launch(self):
        for name in ('attempt.json', 'outer-attempt.json', 'supervised', 'outer-guardian.stdout'):
            with self.subTest(name=name):
                p = self.out/name
                p.write_bytes(b'SYNTHETIC')
                with self.assertRaisesRegex(ValueError, 'already consumed'):
                    d.fresh(self.out)
                p.unlink()

    def test_missing_verifier_report_retains_raw_failure_and_missing_artifacts(self):
        self.rows['verification']['raw_exit_code'] = 2
        def empty(phase, _):
            if phase == 'verification':
                (self.out/'outer-verification.stdout').write_bytes(b'')
                (self.out/'crypto/ledger.jsonl').unlink()
        self.effect = empty
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])
        self.assertEqual(r['primary_failure'], 'child_nonzero')
        self.assertEqual(r['phases']['verification']['raw_exit_code'], 2)
        self.assertTrue({'invalid_verification_report', 'missing_crypto_ledger'} <=
                        {x['reason'] for x in r['failures']})

    def test_actual_accepted_validator_rejects_guardian_post_exit_error(self):
        h = crypto_fixtures.CryptoProfile('test_explicit_profile_and_bound_sources')
        h._testMethodName = 'd2-real-validator-synthetic-bundle'
        h.setUp()
        self.addCleanup(h.doCleanups)
        out = h.bundle()
        self.assertTrue(crypto_fixtures.verify.audit(out)['final_accepted'])
        g = h.helper.get(out, 'guardian.json')
        g.update(exit_code=0, stop='guardian_error')
        h.helper.put(out, 'guardian.json', g)
        h.helper.reseal(out)
        self.audit = crypto_fixtures.verify.audit(out)
        self.assertFalse(self.audit['final_accepted'])
        r, _ = self.run_model()
        self.assertFalse(r['accepted'])

    def test_source_argv_environment_and_policy_frozen_in_intent(self):
        # checked() itself, with a pure computed contract, must reject any frozen-field change.
        source = b'SYNTHETIC SOURCE'
        intent = dict(self.intent, environment={'GOMAXPROCS': '4'}, run_id='a'*32)
        root = self.out/'outer-sources'
        root.mkdir()
        (root/'synthetic.py').write_bytes(source)
        for field in ('commands', 'environment', 'run_id', 'outer_seconds', 'config_sha256'):
            altered = copy.deepcopy(intent)
            altered[field] = 'changed'
            self.helper.put(self.out, 'outer-intent.json', altered)
            with self.subTest(field=field), \
                    mock.patch.object(d, 'contract', return_value=(self.m, intent, {'synthetic.py': source})), \
                    self.assertRaisesRegex(ValueError, 'frozen intent'):
                d.checked(self.config)

    def test_host_preflight_thresholds_fail_closed_without_spawning(self):
        intent = dict(target=dict(path='/SYNTHETIC/target'), minimum_available_bytes=5*1024**3,
                      disk_minimum_bytes=2*1024**3)
        for change in ('memory', 'disk', 'load', 'cpus', 'competing', 'children'):
            with self.subTest(change=change), \
                    mock.patch.object(d, 'available', return_value=(4 if change == 'memory' else 5)*1024**3), \
                    mock.patch.object(os, 'statvfs', return_value=SimpleNamespace(f_bavail=1 if change == 'disk' else 2*1024**3, f_frsize=1)), \
                    mock.patch.object(os, 'getloadavg', return_value=(4 if change == 'load' else 0, 0, 0)), \
                    mock.patch.object(os, 'sched_getaffinity', return_value=set(range(3 if change == 'cpus' else 4))), \
                    mock.patch.object(d, 'competing', return_value=[432] if change == 'competing' else []), \
                    mock.patch.object(d.harness, 'children', return_value=[432] if change == 'children' else []), \
                    mock.patch.object(os, 'pidfd_open', return_value=99), mock.patch.object(os, 'close'), \
                    self.assertRaises(ValueError):
                d.preflight(intent)


class ChildOwnership(unittest.TestCase):
    """Mock syscall protocol, not claims of real kernel cleanup coverage."""
    def setUp(self):
        self.helper = fixtures.G4FinalEvidence('test_valid_complete_bundle')
        self.helper._testMethodName = 'd2-' + self._testMethodName
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.out = self.helper.root

    def exercise(self, event=True, deadline=10, residual=None, identity=True, cleanup_error=False):
        owner = dict(pid=432, pgid=432, sid=432, ppid=os.getpid(), state='S', start_ticks=12)
        image = dict(path='/synthetic/python', device=1, inode=2, sha256='a'*64)
        proc = mock.Mock(pid=432, returncode=None)
        def cleanup(p, signals):
            self.assertIs(p, proc)
            proc.wait.assert_not_called()  # Leader remains reserved until accepted helper takes over.
            proc.returncode = -15 if not event else 0
            if cleanup_error:
                raise RuntimeError('cleanup identity unknown')
            return residual or []
        with mock.patch.object(d.subprocess, 'Popen', return_value=proc) as popen, \
                mock.patch.object(os, 'pidfd_open', return_value=99), mock.patch.object(os, 'close'), \
                mock.patch.object(d.ps, 'identity', return_value=owner if identity else None), \
                mock.patch.object(d.ps, 'executable', return_value=image), \
                mock.patch.object(d.time, 'monotonic_ns', return_value=5), \
                mock.patch.object(os, 'waitid', return_value=SimpleNamespace(si_code=os.CLD_EXITED, si_status=0) if event else None), \
                mock.patch.object(d.harness, 'children', return_value=[432] + (residual or [])), \
                mock.patch.object(d.harness, 'cleanup', side_effect=cleanup) as close:
            r = d.child(['SYNTHETIC-NOT-EXECUTED'], 'guardian',
                        dict(paths=dict(out=str(self.out)), cwd=str(b.REPO), environment={}, python=image), deadline, [])
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        close.assert_called_once()
        return r

    def test_wnowait_and_cleanup_ownership_on_normal_exit(self):
        r = self.exercise()
        self.assertTrue(r['cleanup_complete'] and r['exited_before_deadline'])
        self.assertEqual(r['raw_exit_code'], 0)
        self.assertEqual(r['owner']['start_ticks'], 12)

    def test_hang_observes_deadline_before_wait_and_keeps_signal_exit(self):
        r = self.exercise(event=False, deadline=5)
        self.assertEqual(r['stop'], 'outer_timeout')
        self.assertEqual(r['raw_exit_code'], -15)
        self.assertEqual(r['signal'], 15)
        self.assertFalse(r['exited_before_deadline'])

    def test_early_guardian_exit_with_adopted_descendant(self):
        r = self.exercise(residual=[433])
        self.assertEqual(r['stop'], 'unexpected_descendants')
        self.assertFalse(r['cleanup_complete'])

    def test_unknown_generation_fails_closed_without_numeric_signals(self):
        r = self.exercise(identity=False, cleanup_error=True)
        self.assertEqual(r['stop'], 'collector_exception')
        self.assertFalse(r['cleanup_complete'])
        self.assertEqual(r['raw_exit_code'], 0)  # Cleanup error did not erase the exit.
        self.assertEqual(r['errors'], ['PolicyError', 'cleanup:RuntimeError'])

    def test_accepted_helper_skips_unknown_identity_without_signaling(self):
        with mock.patch.object(d.harness, 'children', return_value=[432]), \
                mock.patch.object(d.harness.processes, 'identity', return_value=None), \
                mock.patch.object(os, 'pidfd_open') as opened, \
                mock.patch.object(signal, 'pidfd_send_signal') as sent:
            d.harness.signal_children(signal.SIGTERM, [])
        opened.assert_not_called()
        sent.assert_not_called()

    def test_no_new_signal_owner_or_arbitrary_command_cli(self):
        tree = ast.parse((b.TOOLS/'d2_collect.py').read_text())
        calls = [ast.unparse(n.func) for n in ast.walk(tree) if isinstance(n, ast.Call)]
        self.assertFalse({'os.kill', 'os.killpg', 'signal.pidfd_send_signal'} & set(calls))
        self.assertEqual(calls.count('subprocess.Popen'), 1)
        self.assertIn('harness.cleanup', calls)
        self.assertIn('os.waitid', calls)
        self.assertIn('WNOWAIT', (b.TOOLS/'d2_collect.py').read_text())
        self.assertNotIn("add_argument('--command'", (b.TOOLS/'d2_collect.py').read_text())
