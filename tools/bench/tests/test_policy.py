"""Pure G2 filesystem/parser regressions. Fabricated outcomes are not run evidence."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from tools.bench import policy_binding as b, policy_launch, policy_verify as v


class PolicyBinding(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dzkr-g2-pure-', dir=os.environ.get('DZKR_BENCH_TEST_OUT'))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.policy = json.loads((b.TOOLS / 'policies/synthetic-v1.json').read_text())
        self.input = self.root / 'input.json'
        self.input.write_bytes(b.encoded(self.policy))
        self.argv = [sys.executable, '-B', str(b.TOOLS / 'tests/fixtures/peer.py'), 'trivial']
        # A supposedly pure test cannot accidentally become a lifecycle test.
        for target in ('subprocess.Popen', 'os.killpg', 'signal.pidfd_send_signal', 'ctypes.CDLL'):
            patch = mock.patch(target, create=True, side_effect=AssertionError('process action in pure test'))
            patch.start()
            self.addCleanup(patch.stop)

    def prepare(self):
        return b.prepare(self.input, self.root / 'run', self.argv, 'a' * 32)

    def replace(self, path, value):
        path.unlink()
        path.write_bytes(b.encoded(value))

    def fabricated_completed(self):
        """Tiny local fixtures for verifier logic, deliberately never launched."""
        out = self.prepare()
        m, p = b.check_binding(out)
        request = dict(schema=1, run_id=m['run_id'], state='launch_requested',
                       manifest_sha256=b.sha(b.read_regular(out / 'manifest.json')))
        (out / 'execution.json').write_bytes(b.encoded(request))
        inner = out / 'supervised/inner'
        inner.mkdir(parents=True)
        for name in ('stdout', 'stderr'):
            (inner / name).write_bytes(b'')
        for name in ('resources.jsonl', 'runner.stdout', 'runner.stderr'):
            (out / 'supervised' / name).write_bytes(b'')
        owner = dict(pid=1234, pgid=1234, sid=1234, start_ticks=123)
        pre = dict(valid=True, live=True, identity=owner, classification='pre_release', rss_bytes=4096)
        (out / 'supervised/resources.jsonl').write_bytes(b.encoded(pre).replace(b'\n', b'') + b'\n')
        limits = {k: x for k, x in b.FIXED.items() if k != 'core_bytes'}
        limits['wall_seconds'] = float(p['wall_seconds'])
        started = dict(argv=m['target_argv'], cwd=m['cwd'], limits=limits,
                       environment={k: m['environment'][k] for k in ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS')}, opt_in_flags={})
        cleanup = dict(success=True, direct_reaped=True, direct_exit_code=0, errors=[], final=dict(live=[], errors=[]))
        result = dict(started, owner=owner, primary=dict(outcome='command_success'), cleanup=cleanup,
                      evidence_complete=True, evidence_errors=[], runner_signals=[], stop_escalation=False,
                      overall_success=True, raw={n: dict(bytes=0, sha256=b.sha(b'')) for n in ('stdout', 'stderr')})
        for name, data in [('started.json', started), ('owner.json', owner), ('result.json', result)]:
            (inner / name).write_bytes(b.encoded(data))
        safe = {k: x for k, x in m['environment'].items() if k in ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS',
                'DZKR_MIMCCHAIN_START_BARRIER', 'DZKR_MIMCCHAIN_BARRIER_TIMEOUT', 'DZKR_MIMCCHAIN_BARRIER_PROBE')}
        supervisor = dict(runner_sha256=b.APPROVED['run_check.py'], supervisor_sha256=b.APPROVED['supervise.py'],
                          target_argv=m['target_argv'], runner_argv=m['runner_argv'], role='probe',
                          barrier_seconds=p['barrier_seconds'], outer_seconds=p['outer_seconds'], inner_seconds=p['wall_seconds'],
                          safe_environment=safe, runner_owner=dict(owner, pid=2222, pgid=2222, sid=2222), target_owner=owner,
                          pre_release_live_sample=pre,
                          inner_result=result, inner_primary=result['primary'], inner_cleanup=result['cleanup'],
                          success=True, reason=None, errors=[], observation_complete=True,
                          cleanup=dict(success=True, residual_live=[], direct_reaped=True, errors=[], runner_exit_code=0))
        (out / 'supervised/supervisor.json').write_bytes(b.encoded(supervisor))
        self.record_outcome(out)
        return out

    def record_outcome(self, out):
        m, p = b.check_binding(out)
        path = out / 'outcome.json'
        if path.exists():
            path.unlink()
        path.write_bytes(b.encoded(v.observed_outcome(out, m, p)))

    def rebind_artifacts(self, out, claimed):
        # Preserve the old success claim; update only raw artifact hashes. This
        # deliberately does NOT ask observed_outcome to supply expected results.
        claimed = copy.deepcopy(claimed)
        for name in claimed['artifacts']:
            raw = (out / name).read_bytes()
            claimed['artifacts'][name] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        self.replace(out / 'outcome.json', claimed)

    def cli(self, out):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, 'argv', ['policy_verify', str(out)]), \
                mock.patch.object(sys, 'stdout', stdout), mock.patch.object(sys, 'stderr', stderr):
            code = v.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def assert_invalid_evidence(self, out):
        with self.assertRaises(b.PolicyError):
            v.verify(out)
        code, stdout, stderr = self.cli(out)
        self.assertEqual(code, 2)
        self.assertEqual(stdout, '')
        self.assertIn('invalid policy evidence:', stderr)
        self.assertNotIn('Traceback', stderr)

    def test_runner_exit_requires_strict_integer_zero(self):
        out = self.fabricated_completed()
        path = out / 'supervised/supervisor.json'
        original = json.loads(path.read_text())
        claimed = json.loads((out / 'outcome.json').read_text())
        missing = object()
        for label, status in [('zero', 0), ('positive', 1), ('negative', -1), ('missing', missing),
                              ('null', None), ('true', True), ('false', False), ('string', '0'),
                              ('float', 0.0), ('list', []), ('object', {})]:
            with self.subTest(label=label):
                s = copy.deepcopy(original)
                if status is missing:
                    del s['cleanup']['runner_exit_code']
                else:
                    s['cleanup']['runner_exit_code'] = status
                self.replace(path, s)
                self.rebind_artifacts(out, claimed)
                if label == 'zero':
                    self.assertTrue(v.verify(out)['run_succeeded'])
                    self.assertEqual(self.cli(out)[0], 0)
                else:
                    # Even coherent self-reported success cannot override the
                    # raw Runner status, even with matching artifact hashes.
                    self.assertTrue(s['success'])
                    self.assertEqual(s['inner_result']['primary']['outcome'], 'command_success')
                    self.assert_invalid_evidence(out)

    def test_successful_target_does_not_hide_runner_failure(self):
        out = self.fabricated_completed()
        path = out / 'supervised/supervisor.json'
        s = json.loads(path.read_text())
        for status in (1, -1):
            with self.subTest(status=status):
                s['cleanup']['runner_exit_code'] = status
                self.replace(path, s)
                m, p = b.check_binding(out)
                actual = v.observed_outcome(out, m, p)
                self.assertEqual(actual['state'], 'runner_failed_or_stopped')
                self.assertIs(actual['run_succeeded'], False)
                self.assertTrue(s['success'])
                self.assertEqual(s['inner_cleanup']['direct_exit_code'], 0)
                self.record_outcome(out)
                self.assertFalse(v.verify(out)['run_succeeded'])
                self.assertEqual(self.cli(out)[0], 1)

    def test_malformed_cleanup_containers_are_controlled_invalid_evidence(self):
        out = self.fabricated_completed()
        spath, ipath = out / 'supervised/supervisor.json', out / 'supervised/inner/result.json'
        original_s, original_i = json.loads(spath.read_text()), json.loads(ipath.read_text())
        claimed = json.loads((out / 'outcome.json').read_text())
        missing = object()
        for scope in ('supervisor', 'target', 'final'):
            for label, bad in [('null', None), ('list', []), ('string', 'bad'), ('number', 0),
                               ('bool', True), ('missing', missing)]:
                with self.subTest(scope=scope, label=label):
                    s, i = copy.deepcopy(original_s), copy.deepcopy(original_i)
                    parent, key = ((s, 'cleanup') if scope == 'supervisor' else
                                   (i, 'cleanup') if scope == 'target' else (i['cleanup'], 'final'))
                    if bad is missing:
                        del parent[key]
                    else:
                        parent[key] = bad
                    s.update(inner_result=i, inner_cleanup=i.get('cleanup'), inner_primary=i['primary'])
                    self.replace(ipath, i)
                    self.replace(spath, s)
                    self.rebind_artifacts(out, claimed)
                    self.assert_invalid_evidence(out)

    def test_missing_or_malformed_cleanup_fields_are_invalid(self):
        out = self.fabricated_completed()
        spath, ipath = out / 'supervised/supervisor.json', out / 'supervised/inner/result.json'
        original_s, original_i = json.loads(spath.read_text()), json.loads(ipath.read_text())
        claimed = json.loads((out / 'outcome.json').read_text())
        for scope, fields in [('supervisor', ('success', 'direct_reaped', 'errors', 'residual_live')),
                              ('target', ('success', 'direct_reaped', 'errors', 'direct_exit_code')),
                              ('final', ('live', 'errors'))]:
            for field in fields:
                for missing in (False, True):
                    with self.subTest(scope=scope, field=field, missing=missing):
                        s, i = copy.deepcopy(original_s), copy.deepcopy(original_i)
                        obj = s['cleanup'] if scope == 'supervisor' else i['cleanup'] if scope == 'target' else i['cleanup']['final']
                        if missing:
                            del obj[field]
                        else:
                            obj[field] = None
                        s.update(inner_result=i, inner_cleanup=i['cleanup'], inner_primary=i['primary'])
                        self.replace(ipath, i)
                        self.replace(spath, s)
                        self.rebind_artifacts(out, claimed)
                        self.assert_invalid_evidence(out)

    def test_malformed_related_nested_records_are_controlled(self):
        out = self.fabricated_completed()
        spath, ipath = out / 'supervised/supervisor.json', out / 'supervised/inner/result.json'
        original_s, original_i = json.loads(spath.read_text()), json.loads(ipath.read_text())
        claimed = json.loads((out / 'outcome.json').read_text())
        for field in ('primary', 'raw'):
            for bad in (None, [], 'bad'):
                with self.subTest(field=field, bad=bad):
                    s, i = copy.deepcopy(original_s), copy.deepcopy(original_i)
                    i[field] = bad
                    s.update(inner_result=i, inner_cleanup=i['cleanup'], inner_primary=i['primary'])
                    self.replace(ipath, i)
                    self.replace(spath, s)
                    self.rebind_artifacts(out, claimed)
                    self.assert_invalid_evidence(out)

    def test_valid_schema(self):
        self.assertEqual(b.parse_policy(self.input.read_bytes()), self.policy)

    def test_bad_json_duplicate_missing_unknown_and_versions(self):
        for raw in (b'', b'{', b'null', b'[]', b'\xff', b'{}', b'{} trailing',
                    b'{"schema":1,"schema":1}', b'NaN', b'1' * 16385):
            with self.subTest(raw=raw[:30]), self.assertRaises(b.PolicyError):
                b.parse_policy(raw)
        for value in (dict(self.policy, schema=2), dict(self.policy, schema=True), dict(self.policy, unsafe=True),
                      {k: x for k, x in self.policy.items() if k != 'cpu_seconds'}):
            with self.subTest(value=value), self.assertRaises(b.PolicyError):
                b.parse_policy(b.encoded(value))
        raw = b.encoded(self.policy).replace(b'"compile": 30', b'"compile": 30, "compile": 31')
        with self.assertRaises(b.PolicyError):
            b.parse_policy(raw)

    def test_numeric_bounds_and_fixed_limits(self):
        for value in (0, -.1, True, '1', None, .001, 211, float('inf'), float('nan'), 10 ** 1000):
            with self.subTest(value=str(value)[:30]), self.assertRaises(b.PolicyError):
                b.parse_policy(json.dumps(dict(self.policy, wall_seconds=value)).encode())
        for key in b.FIXED:
            p = copy.deepcopy(self.policy)
            p[key] = None
            with self.subTest(key=key), self.assertRaises(b.PolicyError):
                b.parse_policy(b.encoded(p))
        with self.assertRaises(b.PolicyError):
            b.parse_policy(b.encoded(dict(self.policy, outer_seconds=2)))

    def test_exact_bytes_hash_size_permissions_and_mapping(self):
        raw = b'  ' + self.input.read_bytes() + b'\n'
        self.input.write_bytes(raw)
        out = self.prepare()
        m, policy = b.check_binding(out)
        self.assertEqual((out / 'policy.snapshot.json').read_bytes(), raw)
        self.assertEqual(m['policy']['sha256'], b.sha(raw))
        self.assertEqual(m['policy']['bytes'], len(raw))
        self.assertEqual((out / 'policy.snapshot.json').stat().st_mode & 0o777, 0o400)
        self.assertEqual(out.stat().st_mode & 0o777, 0o700)
        self.assertEqual(m['runner_argv'], b.runner_argv(m, policy))
        self.assertEqual(m['runner_argv'][m['runner_argv'].index('--wall-seconds') + 1], '1.0')
        self.assertEqual(m['target_argv'], self.argv)

    def test_post_snapshot_input_replacement_does_not_change_authority(self):
        out = self.prepare()
        m, policy = b.check_binding(out)
        self.input.unlink()
        self.input.write_bytes(b'not a policy anymore')
        self.assertEqual(b.check_binding(out), (m, policy))

    def test_input_missing_symlink_fifo_and_hardlink_rejected(self):
        self.input.unlink()
        with self.assertRaises(FileNotFoundError):
            self.prepare()
        other = self.root / 'other'
        other.write_bytes(b.encoded(self.policy))
        self.input.symlink_to(other)
        with self.assertRaises(OSError):
            self.prepare()
        self.input.unlink()
        os.mkfifo(self.input)
        with self.assertRaises(b.PolicyError):
            self.prepare()
        self.input.unlink()
        os.link(other, self.input)
        with self.assertRaises(b.PolicyError):
            self.prepare()

    def test_detect_change_while_reading(self):
        changed = mock.Mock(wraps=self.input.stat())
        changed.st_ino = self.input.stat().st_ino + 1
        with mock.patch.object(b.os, 'stat', return_value=changed), self.assertRaises(b.PolicyError):
            b.read_regular(self.input)

    def test_invalid_ids_and_repeated_output(self):
        for run_id in ('', 'bad', 'A' * 32, 123):
            with self.subTest(run_id=run_id), self.assertRaises(b.PolicyError):
                b.prepare(self.input, self.root / 'run', self.argv, run_id)
        self.prepare()
        with self.assertRaises(FileExistsError):
            self.prepare()

    def test_prepared_is_not_executed(self):
        out = self.prepare()
        status = v.verify(out, prepared=True)
        self.assertEqual(status['state'], 'prepared')
        self.assertFalse(status['run_succeeded'])
        with self.assertRaises(FileNotFoundError):
            v.verify(out)

    def test_substituted_snapshot_hash_size_symlink(self):
        out = self.prepare()
        snapshot = out / 'policy.snapshot.json'
        snapshot.unlink()
        snapshot.write_bytes(b.encoded(dict(self.policy, wall_seconds=1.1)))
        with self.assertRaises(b.PolicyError):
            b.check_binding(out)
        snapshot.unlink()
        snapshot.symlink_to(self.input)
        with self.assertRaises(OSError):
            b.check_binding(out)

    def test_manifest_contradictions_rejected(self):
        out = self.prepare()
        original = json.loads((out / 'manifest.json').read_text())
        for defect in ('hash', 'size', 'limits', 'argv', 'target', 'version', 'run_id', 'environment', 'sources', 'time'):
            m = copy.deepcopy(original)
            if defect == 'hash': m['policy']['sha256'] = '0' * 64
            elif defect == 'size': m['policy']['bytes'] += 1
            elif defect == 'limits': m['effective_limits']['wall_seconds'] = 1.1
            elif defect == 'argv': m['runner_argv'][m['runner_argv'].index('--wall-seconds') + 1] = '2'
            elif defect == 'target': m['target_argv'].append('changed')
            elif defect == 'version': m['schema'] = 2
            elif defect == 'run_id': del m['run_id']
            elif defect == 'environment': m['environment']['GOMAXPROCS'] = '8'
            elif defect == 'sources': del m['tool_sources']['run_check.py']
            else: m['launch_intent_utc'] = 'not-a-time'
            self.replace(out / 'manifest.json', m)
            with self.subTest(defect=defect), self.assertRaises(b.PolicyError):
                b.check_binding(out)

    def test_valid_fabricated_outcome_verifier(self):
        out = self.fabricated_completed()
        self.assertTrue(v.verify(out)['run_succeeded'])
        with self.assertRaises(b.PolicyError):
            v.verify(out, prepared=True)

    def test_spawn_failure_is_not_completion(self):
        out = self.fabricated_completed()
        path = out / 'supervised/supervisor.json'
        s = json.loads(path.read_text())
        s.update(runner_owner=None, target_owner=None, success=False,
                 inner_result=None, inner_primary=None, inner_cleanup=None,
                 pre_release_live_sample=None, observation_complete=False,
                 reason='monitor_or_protocol_failure', errors=[dict(type='OSError')])
        # Matches supervisor.finish(None): no Runner means no Runner exit field.
        s['cleanup'] = dict(success=True, direct_reaped=None, residual_live=[], errors=[], signals=[])
        self.replace(path, s)
        for path in (out / 'supervised/inner').iterdir():
            path.unlink()
        (out / 'supervised/resources.jsonl').write_bytes(b'')
        self.record_outcome(out)
        result = v.verify(out)
        self.assertEqual(result['state'], 'spawn_failed')
        self.assertFalse(result['runner_spawned'])
        self.assertFalse(result['run_succeeded'])

    def test_contradictory_actual_argv_cannot_be_resealed(self):
        out = self.fabricated_completed()
        path = out / 'supervised/supervisor.json'
        value = json.loads(path.read_text())
        value['runner_argv'][value['runner_argv'].index('--wall-seconds') + 1] = '2'
        self.replace(path, value)
        with self.assertRaises(b.PolicyError):
            self.record_outcome(out)

    def test_actual_limits_cannot_be_resealed(self):
        out = self.fabricated_completed()
        path = out / 'supervised/inner/result.json'
        result = json.loads(path.read_text())
        result['limits']['rss_stop_kib'] *= 2
        self.replace(path, result)
        path = out / 'supervised/supervisor.json'
        s = json.loads(path.read_text())
        s['inner_result'] = result
        self.replace(path, s)
        with self.assertRaises(b.PolicyError):
            self.record_outcome(out)

    def test_missing_corrupt_and_forged_outcome(self):
        out = self.fabricated_completed()
        path = out / 'outcome.json'
        original = json.loads(path.read_text())
        self.replace(path, dict(original, run_id='b' * 32))
        with self.assertRaises(b.PolicyError):
            v.verify(out)
        self.replace(path, dict(original, run_succeeded=1))
        with self.assertRaises(b.PolicyError):
            v.verify(out)
        path.unlink()
        path.write_bytes(b'{')
        with self.assertRaises(b.PolicyError):
            v.verify(out)
        (out / 'supervised/inner/stdout').unlink()
        self.record_outcome(out)
        self.assertEqual(v.verify(out)['state'], 'evidence_incomplete')
        self.assertFalse(v.verify(out)['run_succeeded'])

    def test_failed_timeout_cleanup_and_incomplete_never_succeed(self):
        out = self.fabricated_completed()
        ipath, spath = out / 'supervised/inner/result.json', out / 'supervised/supervisor.json'
        initial_i, initial_s = json.loads(ipath.read_text()), json.loads(spath.read_text())
        for defect, state in [('nonzero', 'runner_failed_or_stopped'), ('timeout', 'runner_failed_or_stopped'),
                              ('resource', 'runner_failed_or_stopped'), ('cleanup', 'cleanup_failed'),
                              ('evidence', 'evidence_incomplete')]:
            i, s = copy.deepcopy(initial_i), copy.deepcopy(initial_s)
            if defect in ('nonzero', 'timeout', 'resource'):
                i.update(overall_success=False, primary=dict(outcome={'nonzero': 'command_failed', 'timeout': 'timeout', 'resource': 'resource_stop'}[defect]))
                i['cleanup']['direct_exit_code'] = 7
            elif defect == 'cleanup':
                i['cleanup']['success'] = False
            else:
                i['evidence_complete'] = False
            s.update(inner_result=i, inner_primary=i['primary'], inner_cleanup=i['cleanup'], success=False)
            self.replace(ipath, i)
            self.replace(spath, s)
            self.record_outcome(out)
            with self.subTest(defect=defect):
                result = v.verify(out)
                self.assertEqual(result['state'], state)
                self.assertFalse(result['run_succeeded'])

    def test_preflight_rejection_never_enters_supervisor(self):
        out = self.prepare()
        snapshot = out / 'policy.snapshot.json'
        snapshot.unlink()
        snapshot.write_bytes(b'{}')
        with mock.patch.object(policy_launch.supervise, 'supervise') as start:
            with self.assertRaises(b.PolicyError):
                policy_launch.launch(out)
            start.assert_not_called()
        self.assertFalse((out / 'execution.json').exists())

    def test_source_identity_change_prevents_launch(self):
        out = self.prepare()
        real = b.read_regular
        def read(path, *args):
            if Path(path) == b.TOOLS / 'run_check.py': return b'changed'
            return real(path, *args)
        with mock.patch.object(b, 'read_regular', side_effect=read), mock.patch.object(policy_launch.supervise, 'supervise') as start:
            with self.assertRaises(b.PolicyError):
                policy_launch.launch(out)
            start.assert_not_called()

    def test_adapter_passes_snapshot_values_and_reapplies_sanitized_probe_gate(self):
        out = self.prepare()
        self.input.write_bytes(b'original source changed after freeze')
        def inspect(argv, destination, **kwargs):
            self.assertEqual(argv, self.argv)
            self.assertEqual(destination, out / 'supervised')
            self.assertEqual(kwargs, dict(role='probe', env_overrides={'DZKR_MIMCCHAIN_BARRIER_PROBE': '1'},
                                         barrier_seconds=1.0, inner_seconds=1.0, outer_seconds=8.0))
            self.assertEqual(dict(os.environ), b.ENVIRONMENT)
            raise RuntimeError('pure stop before process creation')
        with mock.patch.dict(os.environ, {'G2_TEST_SECRET': 'synthetic-only'}):
            with mock.patch.object(policy_launch.supervise, 'supervise', side_effect=inspect):
                with self.assertRaisesRegex(RuntimeError, 'pure stop'):
                    policy_launch.launch(out)
            self.assertEqual(os.environ['G2_TEST_SECRET'], 'synthetic-only')
        with mock.patch.object(policy_launch.supervise, 'supervise') as start:
            with self.assertRaises(FileExistsError):
                policy_launch.launch(out)
            start.assert_not_called()

    def test_verification_has_no_original_path_dependency(self):
        out = self.fabricated_completed()
        self.input.unlink()
        self.assertTrue(v.verify(out)['run_succeeded'])


if __name__ == '__main__':
    unittest.main()
