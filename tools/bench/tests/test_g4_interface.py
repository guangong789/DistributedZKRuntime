"""Pure F0 contract/channel/callback tests. No target, Runner or Go is launched."""
import copy
import io
import json
import os
from pathlib import Path
import socket
import struct
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

from tools.bench import g4_binding as binding, g4_bootstrap as bootstrap
from tools.bench import g4_events as events, g4_supervise as supervisor, policy_binding as b
from tools.bench.tests.test_group_rss import OWNER, row

RUN_ID = 'a' * 32


class Connection:
    def __init__(self, frames=()):
        self.blocks = list(frames)
        self.sent, self.timeouts, self.closed = [], [], False

    def recv(self, size):
        if not self.blocks:
            return b''
        value = self.blocks.pop(0)
        if isinstance(value, Exception):
            raise value
        self.blocks.insert(0, value[size:]) if len(value) > size else None
        return value[:size]

    def sendall(self, data):
        self.sent.append(data)

    def settimeout(self, seconds):
        self.timeouts.append(seconds)

    def close(self):
        self.closed = True

    def connect(self, path):
        self.path = path

    def getsockopt(self, *args):
        return self.credentials


def frame(value):
    return (json.dumps(value) + '\n').encode()


def messages():
    state = events.State(RUN_ID, clock=lambda: 1000)
    sample = dict(sequence=0, start_ns=100, end_ns=102, raw_bytes=100, raw_sha256='a' * 64)
    return [state.message('ready_verified', OWNER, stamp=99),
            state.message('readiness_persisted', OWNER, stamp=103, sample=sample),
            state.message('release_initiated', OWNER, stamp=104),
            state.message('release_acknowledged', OWNER, stamp=105),
            state.message('workload_persisted', OWNER, stamp=112,
                          sample=dict(sample, sequence=1, start_ns=110, end_ns=112, raw_bytes=200))]


class G4Interface(unittest.TestCase):
    def setUp(self):
        retained = os.environ.get('DZKR_BENCH_TEST_OUT')
        if retained:
            self.root = Path(retained) / self._testMethodName
            self.root.mkdir(mode=0o700)
        else:
            self.temp = tempfile.TemporaryDirectory(prefix='dzkr-g4-pure-')
            self.addCleanup(self.temp.cleanup)
            self.root = Path(self.temp.name)
        self.input = self.root / 'policy.json'
        self.input.write_bytes((b.TOOLS / 'policies/synthetic-v1.json').read_bytes())
        self.argv = [sys.executable, '-B', '/unused-nonexecuted-target.py']
        for name in ('subprocess.Popen', 'os.killpg', 'signal.pidfd_send_signal', 'ctypes.CDLL'):
            patch = mock.patch(name, create=True, side_effect=AssertionError('process action in pure F0 test'))
            patch.start()
            self.addCleanup(patch.stop)

    def prepare(self):
        return binding.prepare(self.input, self.root / 'run', self.argv, RUN_ID)

    def replace(self, path, value):
        path.unlink()
        path.write_bytes(b.encoded(value))

    def test_legacy_entrypoint_is_default_and_unchanged(self):
        with mock.patch.object(supervisor.legacy, 'supervise', return_value='legacy') as call:
            self.assertEqual(supervisor.supervise(self.argv, self.root / 'old', inner_seconds=1), 'legacy')
            call.assert_called_once_with(self.argv, self.root / 'old', inner_seconds=1)
        self.assertEqual(b.sha((b.TOOLS / 'supervise.py').read_bytes()), b.APPROVED['supervise.py'])

    def test_unapproved_entrypoint_never_falls_back_or_launches(self):
        with mock.patch.object(supervisor.legacy, 'supervise') as old:
            for mode in ('anything', '/bin/sh', '', None):
                with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, 'unapproved'):
                    supervisor.supervise(self.argv, self.root / 'bad', entrypoint=mode)
            with self.assertRaisesRegex(ValueError, 'legacy'):
                supervisor.supervise(self.argv, self.root, config='/wrong')
            old.assert_not_called()

    def test_frozen_binding_and_single_caller_run_id(self):
        with mock.patch('uuid.uuid4', side_effect=AssertionError('second run ID')):
            config = self.prepare()
        m = binding.check(config, live=True)
        self.assertEqual(m['run_id'], RUN_ID)
        self.assertEqual(m['entrypoint'], 'g4-bootstrap-v1')
        self.assertEqual(m['runner_argv'], [m['python']['path'], '-B', '-m',
                         'tools.bench.g4_bootstrap', '--config', str(config)])
        self.assertEqual(m['target_argv'], self.argv)
        self.input.write_text('source policy changed after freeze')
        self.assertEqual(binding.check(config, live=True), m)

    def test_manifest_mutations_rejected_before_launch(self):
        config = self.prepare()
        original = b.decode(config.read_bytes())
        for key, value, reason in (
                ('entrypoint', 'legacy', 'entrypoint'), ('schema', True, 'version'),
                ('run_id', 'wrong', 'run ID'), ('runner_argv', ['/bin/true'], 'argv'),
                ('environment', {}, 'environment'), ('paths', {}, 'paths'),
                ('sources', {}, 'source set'), ('target_executable', None, 'target identity'),
                ('python', None, 'Python identity')):
            with self.subTest(field=key):
                bad = dict(original, **{key: value})
                self.replace(config, bad)
                with self.assertRaisesRegex(ValueError, reason):
                    binding.check(config, live=True)
        self.replace(config, dict(original, unknown=True))
        with self.assertRaisesRegex(ValueError, 'fields'):
            binding.check(config)

    def test_invalid_policy_is_rejected_including_resealed_snapshot(self):
        config = self.prepare()
        original = b.decode(config.read_bytes())
        policy = b.decode(self.input.read_bytes())
        candidates = [b'{"schema":1,"schema":1}', b.encoded(dict(policy, unknown=1)),
                      b.encoded(dict(policy, rss_stop_kib=True)), b.encoded(dict(policy, wall_seconds=0))]
        for raw in candidates:
            with self.subTest(raw_size=len(raw)):
                snapshot = config.parent / 'policy.snapshot.json'
                snapshot.unlink()
                snapshot.write_bytes(raw)
                bad = dict(original, policy=dict(bytes=len(raw), sha256=b.sha(raw)))
                self.replace(config, bad)
                with self.assertRaises(ValueError):
                    binding.check(config, live=True)

    def test_snapshot_hash_mismatch_rejected(self):
        config = self.prepare()
        snapshot = config.parent / 'policy.snapshot.json'
        snapshot.unlink()
        snapshot.write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError, 'snapshot identity'):
            binding.check(config)

    def test_source_digest_and_resealed_live_source_mismatch(self):
        config = self.prepare()
        m = binding.check(config)
        path = config.parent / 'sources/g4_bootstrap.py'
        path.unlink()
        data = b'# substituted\n'
        path.write_bytes(data)
        with self.assertRaisesRegex(ValueError, 'source identity'):
            binding.check(config)
        m['sources']['g4_bootstrap.py'] = dict(bytes=len(data), sha256=b.sha(data))
        self.replace(config, m)
        with self.assertRaisesRegex(ValueError, 'live source'):
            binding.check(config, live=True)

    def test_pinned_runner_cannot_be_resealed(self):
        config = self.prepare()
        m = binding.check(config)
        path = config.parent / 'sources/run_check.py'
        path.unlink()
        path.write_bytes(b'# changed runner\n')
        m['sources']['run_check.py'] = dict(bytes=path.stat().st_size, sha256=b.sha(path.read_bytes()))
        self.replace(config, m)
        with self.assertRaisesRegex(ValueError, 'pinned source'):
            binding.check(config)

    def test_runtime_argv_environment_and_target_identity(self):
        config = self.prepare()
        m = binding.check(config)
        env = dict(m['environment'], DZKR_MIMCCHAIN_BARRIER_SOCKET='/private/socket',
                   DZKR_MIMCCHAIN_BARRIER_TOKEN='a' * 64)
        bootstrap.runtime(m, m['runner_argv'], env, m['cwd'])
        with self.assertRaisesRegex(ValueError, 'argv'):
            bootstrap.runtime(m, ['different'], env, m['cwd'])
        with self.assertRaisesRegex(ValueError, 'environment'):
            bootstrap.runtime(m, m['runner_argv'], dict(env, GOMAXPROCS='99'), m['cwd'])
        with self.assertRaisesRegex(ValueError, 'environment'):
            bootstrap.runtime(m, m['runner_argv'], dict(env, DZKR_MIMCCHAIN_CPU_PILOT='1'), m['cwd'])
        m['target_executable']['sha256'] = '0' * 64
        self.replace(config, m)
        with self.assertRaisesRegex(ValueError, 'live executable'):
            binding.check(config, live=True)

    def test_invalid_run_ids_fail_before_creating_output(self):
        for identity in ('a' * 31, 'G' * 32, True, None):
            with self.subTest(identity=identity), self.assertRaisesRegex(ValueError, 'run ID'):
                binding.prepare(self.input, self.root / 'invalid', self.argv, identity)
        self.assertFalse((self.root / 'invalid').exists())

    def test_valid_causal_exchange_and_inclusive_boundaries(self):
        state = events.State(RUN_ID, clock=lambda: 1000)
        for value in messages():
            state.accept(value)
        self.assertEqual(state.sequence, 5)
        self.assertEqual(state.owner, OWNER)
        self.assertEqual(state.ack_ns, 105)
        state = events.State(RUN_ID, clock=lambda: 1000)
        equal = messages()
        for value in equal:
            value['monotonic_ns'] = 1000
            if value['sample'] is not None:
                value['sample'].update(start_ns=1000, end_ns=1000)
            state.accept(value)
        self.assertEqual(state.sequence, 5)

    def test_wrong_event_run_identity_and_sequence(self):
        for key, value, reason in (('run_id', 'b' * 32, 'run/version'),
                                  ('schema', True, 'run/version'), ('sequence', True, 'sequence'),
                                  ('sequence', 1, 'sequence'), ('state', 'complete', 'order/state')):
            with self.subTest(field=key):
                bad = dict(messages()[0], **{key: value})
                with self.assertRaisesRegex(events.InterfaceError, reason):
                    events.State(RUN_ID, clock=lambda: 1000).accept(bad)
        state = events.State(RUN_ID, clock=lambda: 1000)
        state.bind(OWNER)
        bad = dict(messages()[0], owner=dict(OWNER, start_ticks=8))
        with self.assertRaisesRegex(events.InterfaceError, 'target changed'):
            state.accept(bad)

    def test_duplicate_and_out_of_order_events_rejected(self):
        state = events.State(RUN_ID, clock=lambda: 1000)
        state.accept(messages()[0])
        with self.assertRaisesRegex(events.InterfaceError, 'sequence'):
            state.accept(messages()[0])
        state = events.State(RUN_ID, clock=lambda: 1000)
        state.accept(messages()[0])
        bad = dict(messages()[3], sequence=1)
        with self.assertRaisesRegex(events.InterfaceError, 'order/state'):
            state.accept(bad)

    def test_release_ack_cannot_skip_readiness_persistence(self):
        state = events.State(RUN_ID, clock=lambda: 1000)
        state.accept(messages()[0])
        with self.assertRaisesRegex(events.InterfaceError, 'sequence'):
            state.accept(messages()[3])

    def test_stale_chronology_and_workload_before_ack(self):
        for index, mutate in (
                (1, lambda x: x['sample'].update(start_ns=98)),
                (2, lambda x: x.update(monotonic_ns=101)),
                (3, lambda x: x.update(monotonic_ns=103)),
                (4, lambda x: x['sample'].update(start_ns=104)),
                (4, lambda x: x.update(monotonic_ns=1001))):
            with self.subTest(index=index):
                values = messages()
                state = events.State(RUN_ID, clock=lambda: 1000)
                for value in values[:index]:
                    state.accept(value)
                mutate(values[index])
                with self.assertRaisesRegex(events.InterfaceError, 'chronology|before verified ack'):
                    state.accept(values[index])

    def test_closed_timeout_and_malformed_channels_are_controlled(self):
        for blocks, reason in (([], 'closed'), ([TimeoutError()], 'timeout'),
                               ([b'{bad}\n'], 'JSON'), ([b'x' * 2049], 'bound')):
            with self.subTest(reason=reason):
                link = events.Link(Connection(blocks), RUN_ID, 1000, io.StringIO(), clock=lambda: 100)
                with self.assertRaisesRegex(events.InterfaceError, reason):
                    link.receive('ready_verified')
        conn = Connection()
        link = events.Link(conn, RUN_ID, 100, io.StringIO(), clock=lambda: 100)
        with self.assertRaisesRegex(events.InterfaceError, 'timeout'):
            link.receive('ready_verified')
        self.assertEqual(conn.timeouts, [])

    def test_link_records_frames_bounds_and_explicit_error_state(self):
        conn = Connection([frame(messages()[0])])
        journal = io.StringIO()
        link = events.Link(conn, RUN_ID, 1000, journal, clock=lambda: 100)
        self.assertEqual(link.receive('ready_verified'), messages()[0])
        self.assertLessEqual(conn.timeouts[0], 1e-6)
        link.fail('controlled_failure')
        self.assertTrue(link.state.failed)
        self.assertEqual(json.loads(journal.getvalue().splitlines()[-1])['state'], 'failed')
        link.close()
        self.assertTrue(conn.closed)

    def test_kernel_peer_identity_is_required(self):
        conn = mock.Mock()
        conn.getsockopt.return_value = struct.pack('3i', OWNER['pid'], os.getuid(), os.getgid())
        with mock.patch.object(events.ps, 'identity', return_value=dict(OWNER)):
            events.peer(conn, OWNER)
        conn.getsockopt.return_value = struct.pack('3i', OWNER['pid'] + 1, os.getuid(), os.getgid())
        with self.assertRaisesRegex(events.InterfaceError, 'peer identity'):
            events.peer(conn, OWNER)
        conn.getsockopt.return_value = struct.pack('3i', OWNER['pid'], os.getuid(), os.getgid())
        with mock.patch.object(events.ps, 'identity', return_value=dict(OWNER, start_ticks=8)):
            with self.assertRaisesRegex(events.InterfaceError, 'peer identity'):
                events.peer(conn, OWNER)

    def test_channel_connect_checks_parent_and_confirmation_before_return(self):
        config = self.prepare()
        m = binding.check(config)
        runner = dict(pid=os.getpid(), pgid=os.getpid(), sid=os.getpid(), start_ticks=12)
        parent = dict(pid=os.getppid(), pgid=os.getppid(), sid=os.getppid(), start_ticks=10)
        hello = dict(schema=1, contract=binding.CONTRACT, kind='hello', run_id=RUN_ID,
                     runner=runner, supervisor_pid=parent['pid'], config_sha256=b.sha(config.read_bytes()))
        conn = Connection([frame(dict(hello, kind='connected'))])
        conn.credentials = struct.pack('3i', parent['pid'], os.getuid(), os.getgid())
        lookup = lambda pid: parent if pid == parent['pid'] else runner
        with mock.patch.object(events.socket, 'socket', return_value=conn), \
                mock.patch.object(events.ps, 'identity', side_effect=lookup):
            link = events.connect(m, time.monotonic_ns() + 1_000_000_000, io.StringIO())
        self.assertEqual(conn.path, m['paths']['channel'])
        self.assertEqual(json.loads(conn.sent[0]), hello)
        self.assertEqual(link.peer_owner, parent)
        conn = Connection([])
        conn.credentials = struct.pack('3i', parent['pid'], os.getuid(), os.getgid())
        with mock.patch.object(events.socket, 'socket', return_value=conn), \
                mock.patch.object(events.ps, 'identity', side_effect=lookup):
            with self.assertRaisesRegex(events.InterfaceError, 'closed'):
                events.connect(m, time.monotonic_ns() + 1_000_000_000, io.StringIO())
        self.assertTrue(conn.closed)

    def test_server_rejects_wrong_hello_and_closes_connection(self):
        config = self.prepare()
        m = binding.check(config)
        runner = dict(pid=200, pgid=200, sid=200, start_ticks=9)
        hello = dict(schema=1, contract=binding.CONTRACT, kind='hello', run_id=RUN_ID,
                     runner=runner, supervisor_pid=os.getpid(), config_sha256=b.sha(config.read_bytes()))
        for valid in (True, False):
            conn = Connection([frame(dict(hello, config_sha256=hello['config_sha256'] if valid else '0' * 64))])
            conn.credentials = struct.pack('3i', 200, os.getuid(), os.getgid())
            server = events.Server(m)
            server.listener = mock.Mock()
            server.listener.accept.return_value = (conn, None)
            with mock.patch.object(events.ps, 'identity', return_value=runner):
                if valid:
                    link = server.accept(runner, time.monotonic_ns() + 1_000_000_000, io.StringIO())
                    self.assertEqual(link.peer_owner, runner)
                    self.assertEqual(json.loads(conn.sent[0])['kind'], 'connected')
                    link.close()
                else:
                    with self.assertRaisesRegex(events.InterfaceError, 'hello identity'):
                        server.accept(runner, time.monotonic_ns() + 1_000_000_000, io.StringIO())
                    self.assertEqual(conn.sent, [])
                self.assertTrue(conn.closed)

    def test_missing_ack_never_observes_workload(self):
        config = self.prepare()
        m = binding.check(config)
        Path(m['paths']['supervised']).mkdir()
        Path(m['paths']['inner']).mkdir()
        (Path(m['paths']['inner']) / 'owner.json').write_bytes(b.encoded(OWNER))
        conn = Connection([frame(messages()[3])])
        link = events.Link(conn, RUN_ID, 1000, io.StringIO(), clock=lambda: 100)
        recorder = mock.Mock()
        monitor = bootstrap.Monitor(m, link, recorder)
        with mock.patch.object(bootstrap.group_rss, 'observe') as observe:
            with self.assertRaisesRegex(events.InterfaceError, 'sequence'):
                monitor(OWNER['pid'])
            observe.assert_not_called()
        self.assertIsNotNone(monitor.error)

    def exercise_callback(self, success):
        config = self.prepare()
        m = binding.check(config)
        Path(m['paths']['supervised']).mkdir()
        values = messages()
        for n in (0, 2, 3):
            values[n]['monotonic_ns'] = 1000
        conn = Connection([frame(values[n]) for n in (0, 2, 3)])
        link = events.Link(conn, RUN_ID, 2000, io.StringIO(), clock=lambda: 1000)
        returned = []

        def observe(owner, identity, sequence, phase, site='runner_monitor'):
            result = row(sequence, (1024 * (sequence + 1),), phase['name'])
            result.update(phase=phase, start_ns=1000, end_ns=1000, site=site)
            if site == 'cleanup_check':
                result.update(start_ns=time.monotonic_ns(), end_ns=time.monotonic_ns(),
                              anchor_before=None, anchor_after=None, scan_before=[], scan_after=[],
                              members=[], sample_status='NO_LIVE_MEMBERS', group_rss_bytes=None)
            return result

        def core(argv, out, **options):
            Path(out).mkdir()
            (Path(out) / 'owner.json').write_bytes(b.encoded(OWNER))
            returned.extend(options['monitor'](OWNER['pid'])[0]['rss_kib'] for _ in range(2))
            result = dict(owner=OWNER, sampled_group_peak_rss_kib=2, overall_success=success)
            (Path(out) / 'result.json').write_bytes(b.encoded(result))
            return result

        with mock.patch.object(bootstrap.group_rss, 'observe', side_effect=observe), \
                mock.patch.object(bootstrap.run_check, 'run_command', side_effect=core) as call:
            outcome = bootstrap.execute(m, link)
        call.assert_called_once()
        self.assertEqual(call.call_args.args, (self.argv, m['paths']['inner']))
        self.assertEqual(call.call_args.kwargs['wall_seconds'], m['effective_limits']['wall_seconds'])
        self.assertEqual(returned, [1, 2])
        self.assertEqual(outcome['run_id'], RUN_ID)
        self.assertTrue(outcome['interface_complete'])
        self.assertEqual(outcome['execution_succeeded'], success)
        self.assertEqual(outcome['accepted'], success)
        self.assertFalse(outcome['full_g4_evidence_assessed'])
        raw = Path(m['paths']['raw']).read_bytes()
        rows = [json.loads(x) for x in raw.splitlines()]
        self.assertEqual([x['phase']['name'] for x in rows], ['readiness', 'workload', 'cleanup'])
        self.assertTrue(all(x['run_id'] == RUN_ID for x in rows))
        self.assertEqual(outcome['audit']['runner_monitor_peak_bytes'], 2048)
        receipts = [json.loads(x) for x in conn.sent]
        self.assertEqual([x['event'] for x in receipts], ['readiness_persisted', 'workload_persisted'])
        events.verify_receipt(m, receipts[-1], 'workload')
        return m, receipts

    def test_callback_records_same_samples_and_reuses_original_core(self):
        self.exercise_callback(True)

    def test_valid_unsuccessful_core_is_not_accepted(self):
        self.exercise_callback(False)

    def test_resealed_persisted_row_still_requires_correct_phase_and_identity(self):
        m, receipts = self.exercise_callback(True)
        sample = copy.deepcopy(receipts[-1])
        path = Path(m['paths']['raw'])
        rows = [json.loads(x) for x in path.read_bytes().splitlines()][:2]
        for field in ('phase', 'owner'):
            changed = copy.deepcopy(rows)
            if field == 'phase':
                changed[-1]['phase']['name'] = 'unknown'
            else:
                changed[-1]['owner']['start_ticks'] += 1
            raw = b''.join(frame(x) for x in changed)
            path.write_bytes(raw)
            sample['sample'].update(raw_bytes=len(raw), raw_sha256=b.sha(raw))
            with self.subTest(field=field), self.assertRaisesRegex(events.InterfaceError, 'receipt disagreement'):
                events.verify_receipt(m, sample, 'workload')

    def test_private_server_directory_cleanup_without_process_ownership(self):
        m = dict(paths=dict(channel=str(self.root / 'channel/events.sock')))
        listener = mock.Mock()
        with mock.patch.object(events.socket, 'socket', return_value=listener):
            with events.Server(m):
                self.assertEqual((self.root / 'channel').stat().st_mode & 0o777, 0o700)
            self.assertFalse((self.root / 'channel').exists())
        listener.close.assert_called_once()

    def test_controller_gates_release_on_receipt_and_uses_verified_ack(self):
        m, receipts = self.exercise_callback(True)
        conn = Connection([frame(x) for x in receipts])
        controller = supervisor.Controller(m, None, io.StringIO(), 2000)
        controller.link = events.Link(conn, RUN_ID, 2000, io.StringIO(), clock=lambda: 1000)
        controller.ready(dict(events=[dict(monotonic_ns=1000)]), None, None, OWNER, None)
        self.assertFalse(controller.complete())
        controller.before_release({}, None, None, OWNER, None)
        self.assertEqual(controller.link.state.sequence, 2)
        controller.released(dict(release_begin_ns=1000), None, None, OWNER, None)
        controller.acknowledged(dict(ack_ns=1000), None, None, OWNER, None)
        self.assertTrue(controller.complete())
        self.assertEqual([json.loads(x)['event'] for x in conn.sent],
                         ['ready_verified', 'release_initiated', 'release_acknowledged'])
        controller.close()
        self.assertTrue(conn.closed)

    def test_bootstrap_main_rejects_input_without_launch_or_fallback(self):
        with mock.patch.object(sys, 'argv', ['g4_bootstrap', '--config', '/missing/g4.json']), \
                mock.patch.object(sys, 'stderr', io.StringIO()), \
                mock.patch.object(bootstrap, 'execute') as execute:
            self.assertEqual(bootstrap.main(), 2)
            execute.assert_not_called()

    def exercise_supervisor(self, case='run', *, exit_code=0, omit_exit=False,
                            core_success=True, interface_complete=True, cleanup_success=True,
                            evidence_complete=True, reason=None):
        config = binding.prepare(self.input, self.root / case, self.argv, RUN_ID)
        m = binding.check(config)
        output = Path(m['paths']['supervised'])
        runner = dict(pid=200, pgid=200, sid=200, start_ticks=10)
        target = dict(OWNER)
        expected = {k: m['target_executable'][k] for k in ('path', 'device', 'inode', 'sha256')}
        peer = mock.MagicMock()
        peer.recv.side_effect = [frame(dict(version=1, event='ready', token='a' * 64, role='probe', pid=100)),
                                 frame(dict(version=1, event='released', role='probe', pid=100))]
        peer.getsockopt.return_value = struct.pack('3i', 100, os.getuid(), os.getgid())
        listener = mock.MagicMock()
        listener.__enter__.return_value = listener
        listener.accept.return_value = (peer, None)
        proc = mock.Mock(pid=200)
        proc.wait.return_value = exit_code

        def spawn(command, **kw):
            inner = output / 'inner'
            inner.mkdir()
            (inner / 'owner.json').write_bytes(b.encoded(target))
            (inner / 'result.json').write_bytes(b.encoded(dict(overall_success=core_success, evidence_complete=evidence_complete,
                stop_escalation=False, primary=dict(outcome='command_success'), cleanup=dict(success=True))))
            return proc

        def finish(*args):
            cleanup = supervisor.legacy.finish(*args)
            if omit_exit:
                del cleanup['runner_exit_code']
            if not cleanup_success:
                cleanup.update(success=False, errors=[dict(type='InjectedCleanupError')])
            return cleanup

        pre = dict(identity=target, live=True, valid=True, executable=expected, rss_bytes=1024,
                   system_available_bytes=4 * 1024**3, start_ns=1, end_ns=2)
        with mock.patch.object(events, 'Server'), mock.patch.object(supervisor, 'Controller') as control, \
                mock.patch.object(supervisor.subprocess, 'Popen', side_effect=spawn) as popen, \
                mock.patch.object(supervisor.socket, 'socket', return_value=listener), \
                mock.patch.object(supervisor.ps, 'identity', side_effect=lambda pid: runner if pid == 200 else target), \
                mock.patch.object(supervisor.ps, 'executable', return_value=expected), \
                mock.patch.object(supervisor.ps, 'observe', return_value=pre), \
                mock.patch.object(supervisor.ps, 'members', return_value=[]), \
                mock.patch.object(supervisor.os, 'waitid', side_effect=[None, types.SimpleNamespace()]), \
                mock.patch.object(supervisor.select, 'select'), \
                mock.patch.object(supervisor.signal, 'signal'), \
                mock.patch.object(supervisor, 'finish', side_effect=finish) as cleanup_call, \
                mock.patch.object(supervisor.secrets, 'token_hex', return_value='a' * 64):
            control.return_value.complete.return_value = interface_complete
            control.return_value.link.peer_owner = runner
            if reason is not None:
                control.return_value.acknowledged.side_effect = lambda rec, *args: rec.update(reason=reason)
            result = supervisor.supervise(self.argv, output, entrypoint=binding.ENTRYPOINT, config=config)
        cleanup_call.assert_called_once_with(proc, runner, target)
        proc.wait.assert_called_once_with(timeout=3)
        self.assertEqual(popen.call_args.args[0], m['runner_argv'])
        self.assertEqual(result['runner_argv'], m['runner_argv'])
        self.assertEqual(popen.call_args.kwargs['cwd'], m['cwd'])
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        self.assertTrue(result['cleanup']['direct_reaped'])
        if omit_exit:
            self.assertNotIn('runner_exit_code', result['cleanup'])
        else:
            self.assertIs(type(result['cleanup']['runner_exit_code']), type(exit_code))
            self.assertEqual(result['cleanup']['runner_exit_code'], exit_code)
        self.assertIs(result['cleanup']['success'], cleanup_success)
        self.assertIs(result['inner_result']['overall_success'], core_success)
        self.assertIs(result['inner_result']['evidence_complete'], evidence_complete)
        self.assertIs(result['inner_result']['cleanup']['success'], True)
        self.assertIs(result['inner_result']['stop_escalation'], False)
        self.assertIs(result['g4_interface_complete'], interface_complete)
        self.assertIs(result['observation_complete'], True)
        self.assertEqual(result['reason'], reason)
        self.assertEqual(result['errors'], [])
        self.assertEqual(json.loads((output / 'supervisor.json').read_text()), result)
        self.assertEqual(result['run_id'], RUN_ID)
        self.assertNotEqual(result['supervisor_sha256'], b.APPROVED['supervise.py'])
        self.assertEqual(result['runner_sha256'], b.APPROVED['run_check.py'])
        self.assertIs(supervisor.finish, supervisor.legacy.finish)
        protocol_calls = [call[0] for call in control.return_value.method_calls
                          if call[0] in ('runner_created', 'ready', 'before_release', 'released', 'acknowledged')]
        self.assertEqual(protocol_calls, ['runner_created', 'ready', 'before_release', 'released', 'acknowledged'])
        return result

    def test_derived_supervisor_records_exact_launch_and_reuses_cleanup(self):
        result = self.exercise_supervisor()
        self.assertIs(result['success'], True)

    def test_successful_core_does_not_hide_late_bootstrap_failure(self):
        result = self.exercise_supervisor(exit_code=2)
        self.assertIs(result['success'], False)
        self.assertEqual(result['cleanup']['runner_exit_code'], 2)

    def test_bootstrap_exit_status_requires_strict_integer_zero(self):
        for label, code, omit, success in (
                ('zero', 0, False, True), ('one', 1, False, False),
                ('two', 2, False, False), ('signal', -1, False, False),
                ('missing', 0, True, False), ('none', None, False, False),
                ('true', True, False, False), ('false', False, False, False),
                ('string', '0', False, False), ('float', 0.0, False, False),
                ('list', [], False, False), ('object', {}, False, False)):
            with self.subTest(exit_status=label):
                result = self.exercise_supervisor(label, exit_code=code, omit_exit=omit)
                self.assertIs(result['success'], success)

    def test_zero_bootstrap_exit_does_not_override_existing_failures(self):
        for label, options in (
                ('core', dict(core_success=False)),
                ('interface', dict(interface_complete=False)),
                ('cleanup', dict(cleanup_success=False)),
                ('evidence', dict(evidence_complete=False)),
                ('reason', dict(reason='injected_existing_failure'))):
            with self.subTest(failure=label):
                result = self.exercise_supervisor(label, **options)
                self.assertIs(result['success'], False)


if __name__ == '__main__':
    unittest.main()
