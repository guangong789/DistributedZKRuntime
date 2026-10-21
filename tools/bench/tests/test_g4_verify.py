"""Pure fabricated G4 evidence; no targets, sockets, signals or Go builds."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from tools.bench import g4_binding as binding, g4_events as events, g4_verify as verify
from tools.bench import group_rss, policy_binding as b
from tools.bench.tests.test_group_rss import OWNER, row


class G4FinalEvidence(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get('DZKR_BENCH_TEST_OUT'))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        retained = os.environ.get('DZKR_BENCH_TEST_OUT')
        if retained:
            self.root = Path(retained) / self._testMethodName
            self.root.mkdir()
        for name in ('subprocess.Popen', 'os.killpg', 'signal.pidfd_send_signal', 'ctypes.CDLL', 'socket.socket'):
            patch = mock.patch(name, create=True, side_effect=AssertionError('process action in pure G4 test'))
            patch.start()
            self.addCleanup(patch.stop)

    def put(self, out, name, value):
        path = out / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        path.write_bytes(b.encoded(value))

    def get(self, out, name):
        return b.decode((out / name).read_bytes())

    def reseal(self, out):
        (out / 'g4-artifacts.json').unlink(missing_ok=True)
        verify.seal(out)

    def bundle(self, label):
        policy = self.root / (label + '-policy.json')
        policy.write_bytes((b.TOOLS / 'policies/synthetic-v1.json').read_bytes())
        config = binding.prepare(policy, self.root / label, [sys.executable, '-B', '/not-executed.py'], 'a' * 32)
        out, m = config.parent, binding.check(config)
        target = dict(OWNER, state='S', ppid=200)
        runner = dict(pid=200, pgid=200, sid=200, start_ticks=8, state='S', ppid=300)
        supervisor = dict(pid=300, pgid=300, sid=300, start_ticks=9, state='S', ppid=400)
        target_id = events.identity(target)
        limits = {k: v for k, v in b.FIXED.items() if k != 'core_bytes'}
        limits['wall_seconds'] = 1.0
        base = dict(argv=m['target_argv'], cwd=m['cwd'], limits=limits,
                    environment={k: m['environment'][k] for k in ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS')})
        cleanup = dict(success=True, direct_reaped=True, direct_exit_code=0, errors=[], final=dict(live=[], errors=[]))
        core = dict(base, owner=target, primary=dict(outcome='command_success', waitid_code=os.CLD_EXITED, waitid_status=0), cleanup=cleanup,
                    overall_success=True, evidence_complete=True, evidence_errors=[], runner_signals=[],
                    stop_escalation=False, sampled_group_peak_rss_kib=2, minimum_memavailable_kib=4 * 1024**2,
                    raw={n: dict(bytes=0, sha256=b.sha(b'')) for n in ('stdout', 'stderr')})
        for name, value in (('started', base), ('owner', target), ('result', core)):
            self.put(out, 'supervised/inner/' + name + '.json', value)
        for name in ('supervisor.stdout', 'supervisor.stderr', 'supervised/inner/stdout', 'supervised/inner/stderr'):
            path = out / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'')
        recorder = group_rss.Recorder(out / 'supervised/group-rss.jsonl', m['run_id'])
        rows = [row(0, (1024,), 'readiness'), row(1, (2048,), 'workload'), row(2, (), 'cleanup')]
        rows[0]['phase']['event']['monotonic_ns'] = 99
        rows[1]['phase']['event']['monotonic_ns'] = 105
        rows[2].update(site='cleanup_check', anchor_before=None, anchor_after=None,
                       sample_status='NO_LIVE_MEMBERS', group_rss_bytes=None)
        values, samples = [], []
        for r in rows:
            recorder.append(r)
            samples.append(events.receipt(recorder, r))
        summary = recorder.close(2048)
        self.put(out, 'supervised/group-summary.json', summary)
        state = events.State(m['run_id'], clock=lambda: 10000)
        for name, stamp, sample in (('ready_verified', 99, None), ('readiness_persisted', 103, samples[0]),
                                    ('release_initiated', 104, None), ('release_acknowledged', 105, None),
                                    ('workload_persisted', 112, samples[1])):
            message = state.message(name, target_id, stamp=stamp, sample=sample)
            state.accept(message)
            values.append(message)
        self.transcripts(out, values)
        audit = verify.group_audit.audit(out / 'supervised/group-rss.jsonl', out / 'supervised/group-summary.json')
        image = {k: m['target_executable'][k] for k in ('path', 'device', 'inode', 'sha256')}
        pre = dict(valid=True, live=True, identity=target, start_ns=100, end_ns=101, rss_bytes=1024,
                   classification='pre_release')
        s = dict(run_id=m['run_id'], g4_contract=binding.CONTRACT, g4_config_sha256=b.sha(config.read_bytes()),
                 runner_owner=runner, target_owner=target, peer=dict(pid=100, uid=os.getuid()),
                 runner_sha256=m['sources']['run_check.py']['sha256'], supervisor_sha256=m['sources']['g4_supervise.py']['sha256'],
                 g4_channel_peer=events.identity(runner), runner_argv=m['runner_argv'], target_argv=m['target_argv'],
                 expected_executable=image, target_executable=image,
                 safe_environment={k: m['environment'][k] for k in ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS',
                      'DZKR_MIMCCHAIN_START_BARRIER', 'DZKR_MIMCCHAIN_BARRIER_TIMEOUT', 'DZKR_MIMCCHAIN_BARRIER_PROBE')},
                 inner_seconds=1.0, barrier_seconds=1.0, outer_seconds=8.0,
                 events=[dict(event='target_identified', monotonic_ns=99)], release_begin_ns=104, ack_ns=105,
                 ipc=dict(socket_path='/tmp/not-opened/s', token_sha256='b' * 64), inner_result=core,
                 inner_primary=core['primary'], inner_cleanup=core['cleanup'],
                 cleanup=dict(success=True, direct_reaped=True, residual_live=[], errors=[], runner_exit_code=0),
                 pre_release_live_sample=pre, readiness_rss_bytes=1024, all_live_sampled_peak_bytes=1024,
                 post_release_live_samples=0, post_release_sampled_peak_bytes=None,
                 observation_complete=True, g4_interface_complete=True,
                 success=True, reason=None, errors=[])
        self.put(out, 'supervised/supervisor.json', s)
        (out / 'supervised/resources.jsonl').write_bytes(b.encoded(pre).replace(b'\n', b'') + b'\n')
        self.put(out, 'supervised/bootstrap.json', dict(contract=binding.CONTRACT, run_id=m['run_id'],
                  supervisor_owner=events.identity(supervisor), execution_succeeded=True,
                  interface_complete=True, accepted=True, error=None, audit=audit))
        observations = {}
        for role, identity, argv, executable in (('bootstrap', runner, m['runner_argv'], m['python']),
                                                ('target', target, m['target_argv'], m['target_executable'])):
            observations[role] = dict(identity=identity, argv=argv,
                    executable={k: executable[k] for k in ('path', 'device', 'inode', 'sha256')},
                    environment=dict(m['environment'], DZKR_MIMCCHAIN_BARRIER_SOCKET='/tmp/not-opened/s'),
                    barrier_token_sha256='b' * 64)
        self.put(out, 'guardian.json', dict(run_id=m['run_id'], supervisor_owner=supervisor, observations=observations,
                  argv=[m['python']['path'], '-B', '-m', 'tools.bench.g4_launch', '--supervisor', str(config)],
                  start_ns=90, end_ns=10000, stop=None, signals=[], residual_children=[], exit_code=0,
                  peak_descendants=3, minimum_memavailable_bytes=4 * 1024**3))
        self.put(out, 'attempt.json', dict(run_id=m['run_id'], config_sha256=b.sha(config.read_bytes()), guardian=dict(pid=400)))
        self.reseal(out)
        return out

    def transcripts(self, out, values):
        raw = b''.join((json.dumps(v, sort_keys=True) + '\n').encode() for v in values)
        for name in ('supervisor-events.jsonl', 'supervised/bootstrap-events.jsonl'):
            path = out / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)

    def refresh_raw(self, out, rows):
        raw = b''.join((json.dumps(r, sort_keys=True) + '\n').encode() for r in rows)
        (out / 'supervised/group-rss.jsonl').write_bytes(raw)
        values = [b.decode(x) for x in (out / 'supervisor-events.jsonl').read_bytes().splitlines()]
        for pos, count in ((1, 1), (4, 2)):
            prefix = b''.join(raw.splitlines(keepends=True)[:count])
            values[pos]['sample'].update(raw_bytes=len(prefix), raw_sha256=b.sha(prefix))
        self.transcripts(out, values)
        summary = self.get(out, 'supervised/group-summary.json')
        summary['raw'] = dict(bytes=len(raw), sha256=b.sha(raw))
        self.put(out, 'supervised/group-summary.json', summary)

    def test_valid_complete_bundle(self):
        result = verify.audit(self.bundle('valid'))
        self.assertTrue(result['final_accepted'], result)
        self.assertTrue(result['evidence_valid'])
        self.assertTrue(result['execution_attempted'])
        self.assertTrue(result['execution_succeeded'])
        self.assertEqual(result['chronology'], sorted(result['chronology']))

    def test_semantic_negative_controls(self):
        cases = (
            ('run-id', 'guardian.json', lambda x: x.update(run_id='b' * 32)),
            ('policy', 'g4.json', lambda x: x['policy'].update(sha256='0' * 64)),
            ('source', 'g4.json', lambda x: x['sources']['g4_bootstrap.py'].update(sha256='0' * 64)),
            ('executable', 'g4.json', lambda x: x['target_executable'].update(sha256='0' * 64)),
            ('argv', 'guardian.json', lambda x: x['observations']['bootstrap'].update(argv=['/wrong'])),
            ('environment', 'guardian.json', lambda x: x['observations']['bootstrap']['environment'].update(GOMAXPROCS='99')),
            ('generation', 'guardian.json', lambda x: x['observations']['bootstrap']['identity'].update(start_ticks=999)),
            ('missing-exit', 'supervised/supervisor.json', lambda x: x['cleanup'].pop('runner_exit_code')),
            ('nonzero-exit', 'supervised/supervisor.json', lambda x: x['cleanup'].update(runner_exit_code=2)),
            ('cleanup', 'guardian.json', lambda x: x.update(residual_children=[999])),
            ('contradictory-success', 'supervised/supervisor.json', lambda x: x.update(success=False)),
            ('nested', 'guardian.json', lambda x: x.update(observations=None)),
            ('observation', 'supervised/supervisor.json', lambda x: x.update(observation_complete=False)),
            ('malformed-errors', 'supervised/supervisor.json', lambda x: x.update(errors=None)),
            ('runner-peak', 'supervised/inner/result.json', lambda x: x.update(sampled_group_peak_rss_kib=3)),
        )
        for label, name, change in cases:
            with self.subTest(case=label):
                out = self.bundle(label)
                obj = self.get(out, name)
                change(obj)
                self.put(out, name, obj)
                self.reseal(out)
                result = verify.audit(out)
                self.put(out, 'negative-result.json', result)
                self.assertFalse(result['evidence_valid'], result)
                self.assertFalse(result['final_accepted'])

    def test_transcript_and_raw_negative_controls(self):
        for label in ('owner', 'missing-receipt', 'workload-before-ack', 'chronology', 'missing-raw', 'arithmetic'):
            with self.subTest(case=label):
                out = self.bundle(label)
                values = [b.decode(x) for x in (out / 'supervisor-events.jsonl').read_bytes().splitlines()]
                if label == 'owner':
                    values[1]['owner']['start_ticks'] += 1
                elif label == 'missing-receipt':
                    del values[1]
                elif label == 'workload-before-ack':
                    values[3]['monotonic_ns'] = 111
                elif label == 'chronology':
                    values[2]['monotonic_ns'] = 100
                if label in ('missing-raw', 'arithmetic'):
                    rows = [b.decode(x) for x in (out / 'supervised/group-rss.jsonl').read_bytes().splitlines()]
                    if label == 'missing-raw':
                        del rows[1]
                    else:
                        rows[1]['members'][0]['rss_bytes'] += 1024
                    self.refresh_raw(out, rows)
                else:
                    self.transcripts(out, values)
                self.reseal(out)
                result = verify.audit(out)
                self.put(out, 'negative-result.json', result)
                self.assertFalse(result['evidence_valid'], result)
                if label == 'arithmetic':
                    self.assertIn('group RSS sum disagreement', str(result['errors']))

    def test_missing_and_corrupt_artifacts(self):
        for label in ('missing', 'corrupt'):
            with self.subTest(case=label):
                out = self.bundle(label)
                path = out / 'supervised/bootstrap.json'
                if label == 'missing':
                    path.unlink()
                else:
                    path.write_bytes(b'corrupt')
                result = verify.audit(out)
                self.assertFalse(result['evidence_valid'])
                self.assertFalse(result['final_accepted'])
                self.assertIsNone(result['execution_attempted'])
                self.assertIsNone(result['execution_succeeded'])

    def test_valid_unsuccessful_execution(self):
        out = self.bundle('unsuccessful')
        core = self.get(out, 'supervised/inner/result.json')
        core.update(overall_success=False, primary=dict(outcome='command_failed', waitid_code=os.CLD_EXITED, waitid_status=1))
        core['cleanup']['direct_exit_code'] = 1
        self.put(out, 'supervised/inner/result.json', core)
        s = self.get(out, 'supervised/supervisor.json')
        s.update(inner_result=core, inner_primary=core['primary'], inner_cleanup=core['cleanup'], success=False)
        s['cleanup']['runner_exit_code'] = 1
        self.put(out, 'supervised/supervisor.json', s)
        boot = self.get(out, 'supervised/bootstrap.json')
        boot.update(execution_succeeded=False, accepted=False)
        self.put(out, 'supervised/bootstrap.json', boot)
        guardian = self.get(out, 'guardian.json')
        guardian['exit_code'] = 1
        self.put(out, 'guardian.json', guardian)
        self.reseal(out)
        result = verify.audit(out)
        self.assertTrue(result['evidence_valid'], result)
        self.assertFalse(result['execution_succeeded'])
        self.assertFalse(result['final_accepted'])

    def resource_bundle(self, label, *, rss_kib=2, available_bytes=4 * 1024**3,
                        primary=None, recorded_escalation=None):
        """Reconcile every dependent field so policy controls cannot fail on hashes."""
        out = self.bundle(label)
        rows = [b.decode(x) for x in (out / 'supervised/group-rss.jsonl').read_bytes().splitlines()]
        rows[1]['members'][0]['rss_bytes'] = rows[1]['group_rss_bytes'] = rss_kib * 1024
        rows[1]['mem_available_bytes'] = available_bytes
        self.refresh_raw(out, rows)
        peak = max(r['group_rss_bytes'] for r in rows if r['sample_status'] == 'COMPLETE')
        summary = self.get(out, 'supervised/group-summary.json')
        for key in ('complete_sample_peak_bytes', 'runner_monitor_peak_bytes', 'legacy_runner_peak_bytes'):
            summary[key] = peak
        summary['phases']['workload']['peak_bytes'] = rss_kib * 1024
        self.put(out, 'supervised/group-summary.json', summary)
        core = self.get(out, 'supervised/inner/result.json')
        core['sampled_group_peak_rss_kib'] = peak // 1024
        core['minimum_memavailable_kib'] = min(r['mem_available_bytes'] // 1024
                                              for r in rows if r['site'] == 'runner_monitor')
        core['stop_escalation'] = (peak // 1024 > b.FIXED['stop_escalation_kib']
                                   if recorded_escalation is None else recorded_escalation)
        if primary is not None:
            core['primary'] = primary
        succeeded = core['primary']['outcome'] == 'command_success' and not core['stop_escalation']
        core['overall_success'] = succeeded
        core['cleanup']['direct_exit_code'] = 0 if core['primary']['outcome'] == 'command_success' else -15
        self.put(out, 'supervised/inner/result.json', core)
        s = self.get(out, 'supervised/supervisor.json')
        s.update(inner_result=core, inner_primary=core['primary'], inner_cleanup=core['cleanup'], success=succeeded)
        s['cleanup']['runner_exit_code'] = 0 if succeeded else 1
        self.put(out, 'supervised/supervisor.json', s)
        boot = self.get(out, 'supervised/bootstrap.json')
        boot.update(execution_succeeded=succeeded, accepted=succeeded,
                    audit=verify.group_audit.audit(out / 'supervised/group-rss.jsonl',
                                                   out / 'supervised/group-summary.json'))
        self.put(out, 'supervised/bootstrap.json', boot)
        guardian = self.get(out, 'guardian.json')
        guardian['exit_code'] = 0 if succeeded else 1
        self.put(out, 'guardian.json', guardian)
        self.reseal(out)
        return out

    def resource_result(self, out):
        # Independently validated member sums and current outer hashes are prerequisites.
        self.assertTrue(verify.group_audit.audit(out / 'supervised/group-rss.jsonl',
                                               out / 'supervised/group-summary.json')['contract_valid'])
        result = verify.audit(out)
        self.put(out, 'resource-result.json', result)
        return result

    def test_resource_trigger_rejects_success(self):
        for label, fields in (
                ('low-headroom', dict(available_bytes=1024**3)),
                ('high-rss', dict(rss_kib=3 * 1024**2, recorded_escalation=False))):
            with self.subTest(case=label):
                result = self.resource_result(self.resource_bundle(label, **fields))
                self.assertFalse(result['final_accepted'], result)
                self.assertFalse(result['evidence_valid'])
                self.assertIn('resource policy:', str(result['errors']))

    def test_resource_rss_and_escalation_boundaries(self):
        for key in ('rss_stop_kib', 'stop_escalation_kib'):
            for delta in (-1, 0, 1):
                with self.subTest(limit=key, delta=delta):
                    amount = b.FIXED[key] + delta
                    stopped = amount > b.FIXED['rss_stop_kib']
                    primary = dict(outcome='resource_stop', reason='group RSS') if stopped else None
                    out = self.resource_bundle(f'{key}-{delta}', rss_kib=amount, primary=primary)
                    result = self.resource_result(out)
                    self.assertTrue(result['evidence_valid'], result)
                    self.assertEqual(result['final_accepted'], amount <= b.FIXED['stop_escalation_kib'])
                    self.assertEqual(result['resource_policy']['observed_stop_required'], stopped)
                    self.assertEqual(result['resource_policy']['stop_escalation'],
                                     amount > b.FIXED['stop_escalation_kib'])

    def test_resource_available_boundaries(self):
        limit = b.FIXED['available_stop_kib'] * 1024
        for delta in (-1, 0, 1, 1023):
            with self.subTest(byte_delta=delta):
                stopped = delta < 0
                primary = dict(outcome='resource_stop', reason='system MemAvailable') if stopped else None
                out = self.resource_bundle(f'available-{delta}', available_bytes=limit + delta, primary=primary)
                result = self.resource_result(out)
                self.assertTrue(result['evidence_valid'], result)
                self.assertEqual(result['execution_succeeded'], not stopped)
                self.assertEqual(result['final_accepted'], not stopped)
                self.assertEqual(result['resource_policy']['observed_stop_required'], stopped)

    def test_resource_stop_reason_and_priority(self):
        for label, amount, available, reason, valid in (
                ('wrong-reason', 3 * 1024**2, 4 * 1024**3, 'system MemAvailable', False),
                ('both-rss-priority', 3 * 1024**2, 1024**3, 'group RSS', True),
                ('both-wrong-priority', 3 * 1024**2, 1024**3, 'system MemAvailable', False),
                ('unobserved-stop', 2, 4 * 1024**3, 'group RSS', False)):
            with self.subTest(case=label):
                out = self.resource_bundle(label, rss_kib=amount, available_bytes=available,
                                           primary=dict(outcome='resource_stop', reason=reason))
                result = self.resource_result(out)
                self.assertEqual(result['evidence_valid'], valid, result)
                self.assertFalse(result['final_accepted'])
                if not valid:
                    self.assertIn('resource policy:', str(result['errors']))

    def test_resource_escalation_requires_derived_boolean(self):
        for label, amount, value in (('false', 1048577, False), ('true', 2, True),
                                     ('integer', 2, 0), ('null', 2, None), ('missing', 2, None)):
            with self.subTest(case=label):
                out = self.resource_bundle(label, rss_kib=amount)
                core = self.get(out, 'supervised/inner/result.json')
                if label == 'missing':
                    del core['stop_escalation']
                else:
                    core['stop_escalation'] = value
                self.put(out, 'supervised/inner/result.json', core)
                self.reseal(out)
                result = self.resource_result(out)
                self.assertFalse(result['evidence_valid'], result)
                self.assertIn('resource policy: stop_escalation', str(result['errors']))

    def test_resource_branch_preemption(self):
        # This checks resource reconciliation, not independent phase-clock validation.
        for label, primary, valid in (
                ('parser', dict(outcome='runner_error', error_type='ValueError', message='bad stage marker'), True),
                ('phase', dict(outcome='timeout', reason='phase watchdog: compile'), True),
                ('wall', dict(outcome='timeout', reason='overall wall watchdog'), False),
                ('exit', dict(outcome='command_failed', waitid_code=os.CLD_EXITED, waitid_status=1), False)):
            with self.subTest(case=label):
                out = self.resource_bundle(label, available_bytes=1024**3, primary=primary)
                result = self.resource_result(out)
                self.assertEqual(result['evidence_valid'], valid, result)
                self.assertFalse(result['final_accepted'])
                if valid:
                    self.assertFalse(result['execution_succeeded'])
                    self.assertTrue(result['resource_policy']['observed_stop_required'])
                else:
                    self.assertIn('resource policy: trigger/outcome disagreement', str(result['errors']))

    def test_resource_incomplete_samples_fail_closed(self):
        for label in ('missing-available', 'partial', 'no-workload'):
            with self.subTest(case=label):
                out = self.resource_bundle(label)
                rows = [b.decode(x) for x in (out / 'supervised/group-rss.jsonl').read_bytes().splitlines()]
                if label == 'missing-available':
                    del rows[1]['mem_available_bytes']
                elif label == 'partial':
                    rows[1]['sample_status'] = 'PARTIAL'
                else:
                    del rows[1]
                self.refresh_raw(out, rows)
                self.reseal(out)
                result = verify.audit(out)
                self.put(out, 'resource-result.json', result)
                self.assertFalse(result['evidence_valid'], result)
                self.assertFalse(result['final_accepted'])
                self.assertIsNone(result['execution_succeeded'])

    def test_resource_cleanup_absence_is_not_monitor_input(self):
        out = self.resource_bundle('cleanup')
        rows = [b.decode(x) for x in (out / 'supervised/group-rss.jsonl').read_bytes().splitlines()]
        self.assertIsNone(rows[-1]['group_rss_bytes'])
        rows[-1]['mem_available_bytes'] = 0  # Would trigger a stop if misused as a monitor input.
        self.refresh_raw(out, rows)
        boot = self.get(out, 'supervised/bootstrap.json')
        boot['audit'] = verify.group_audit.audit(out / 'supervised/group-rss.jsonl',
                                               out / 'supervised/group-summary.json')
        self.put(out, 'supervised/bootstrap.json', boot)
        self.reseal(out)
        result = self.resource_result(out)
        self.assertTrue(result['final_accepted'], result)
        self.assertEqual(result['rss_audit']['phases']['workload']['complete'], 1)

    def test_launcher_resource_rejection_precedes_spawn(self):
        from tools.bench import g4_launch
        out = self.bundle('preflight')
        before = (out / 'attempt.json').read_bytes()
        with mock.patch.object(g4_launch, 'available', return_value=0):
            with self.assertRaisesRegex(ValueError, 'resource preflight'):
                g4_launch.launch(out / 'g4.json')
        self.assertEqual((out / 'attempt.json').read_bytes(), before)

    def test_launch_observation_never_records_token(self):
        from tools.bench import g4_launch
        owner = dict(pid=200, pgid=200, sid=200, start_ticks=8, state='S', ppid=300)
        values = [b'python\0-B\0', b'DZKR_MIMCCHAIN_BARRIER_TOKEN=private-token\0LANG=C.UTF-8\0']
        with mock.patch.object(g4_launch.ps, 'identity', return_value=owner), \
                mock.patch.object(g4_launch.ps, 'executable', return_value={'sha256': 'a' * 64}), \
                mock.patch.object(Path, 'read_bytes', side_effect=values):
            result = g4_launch.observe_launch(200)
        self.assertEqual(result['barrier_token_sha256'], b.sha(b'private-token'))
        self.assertEqual(result['environment'], {'LANG': 'C.UTF-8'})
        self.assertNotIn('private-token', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
