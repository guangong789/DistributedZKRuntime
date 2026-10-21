"""SYNTHETIC contract data only: no Go target, IPC, Setup, Prove or Verify."""
import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

from tools.bench import g4_binding as binding, g4_crypto as crypto, g4_verify as verify
from tools.bench import g4_supervise as supervisor, policy_binding as b
from tools.bench.tests import test_g4_verify as g4_tests


class CryptoProfile(unittest.TestCase):
    def setUp(self):
        self.helper = g4_tests.G4FinalEvidence('test_valid_complete_bundle')
        self.helper._testMethodName = 'n8-' + self._testMethodName
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.root = self.helper.root
        (self.root / 'SYNTHETIC-NO-CRYPTO.txt').write_text('Fabricated evidence; not a proof or a proving run.\n')
        self.build = dict(schema=1, profile=crypto.PROFILE,
                          sources={n: dict(bytes=(b.REPO/n).stat().st_size, sha256=b.sha((b.REPO/n).read_bytes()))
                                   for n in crypto.GO_SOURCES},
                          binary=binding.executable(sys.executable), toolchain=binding.executable('/usr/local/go/bin/go'),
                          go_version='go1.27.1', cwd=str(b.REPO), environment=dict(GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOMAXPROCS='4'),
                          exit_code=0)
        # Python's image is deliberately a synthetic stand-in, never executed.
        self.build['argv'] = crypto.build_argv(self.build['toolchain']['path'], self.build['binary']['path'])
        self.build_path = self.root / 'build.json'
        self.build_path.write_bytes(b.encoded(self.build))
        self.policy = b.decode((b.TOOLS/'policies/synthetic-v1.json').read_bytes())
        self.policy.update(wall_seconds=3.0, barrier_seconds=2.0, outer_seconds=8.0)
        self.policy_path = self.root / 'policy.json'
        self.policy_path.write_bytes(b.encoded(self.policy))

    def prepare(self, label='crypto-run'):
        return binding.prepare(self.policy_path, self.root/label,
                               crypto.target_argv(self.build['binary']['path']), 'a'*32,
                               profile=crypto.PROFILE, build_manifest=self.build_path)

    def test_explicit_profile_and_bound_sources(self):
        config = self.prepare()
        m = binding.check(config, live=True)
        self.assertEqual(m['schema'], 2)
        self.assertEqual(m['entrypoint'], crypto.ENTRYPOINT)
        self.assertEqual(binding.launch_role(m), 'pilot')
        self.assertEqual(binding.launch_overrides(m), {'DZKR_MIMCCHAIN_CPU_PILOT': '1'})
        self.assertEqual(m['environment']['DZKR_MIMCCHAIN_CPU_N'], '8')
        self.assertNotIn('DZKR_MIMCCHAIN_BARRIER_PROBE', m['environment'])
        self.assertNotIn('GOMEMLIMIT', m['environment'])
        self.assertEqual(m['crypto']['guardian_seconds'], 15)
        self.assertEqual(m['crypto']['outer_seconds'], 25)
        self.assertEqual(m['crypto']['source_id'], b.sha(self.build_path.read_bytes()))

    def test_synthetic_path_has_no_unbound_crypto_import(self):
        import builtins
        original = builtins.__import__

        def guarded(name, globals=None, locals=None, fromlist=(), level=0):
            if name.endswith('g4_crypto') or 'g4_crypto' in (fromlist or ()):
                raise AssertionError('unbound crypto import on synthetic path')
            return original(name, globals, locals, fromlist, level)

        with mock.patch('builtins.__import__', side_effect=guarded):
            out = self.helper.bundle('synthetic')
            self.assertTrue(verify.audit(out)['final_accepted'])

    def test_prepare_rejects_argv_before_creating_output(self):
        with self.assertRaisesRegex(ValueError, 'argv/timeout profile'):
            binding.prepare(self.policy_path, self.root/'bad-argv', [sys.executable], 'a'*32,
                            profile=crypto.PROFILE, build_manifest=self.build_path)
        self.assertFalse((self.root/'bad-argv').exists())

    def test_unknown_profile_and_implicit_build_rejected(self):
        for kwargs in ({'profile': 'unknown'}, {'build_manifest': self.build_path}, {'profile': crypto.PROFILE}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                binding.prepare(self.policy_path, self.root/'bad', [sys.executable], 'a'*32, **kwargs)
        self.assertFalse((self.root/'bad').exists())

    def test_build_identity_failures(self):
        for label, mutate in (
                ('Go SHA', lambda x: x['sources']['go.mod'].update(sha256='0'*64)),
                ('cwd', lambda x: x.update(cwd='/other')),
                ('binary', lambda x: x['binary'].update(sha256='0'*64)),
                ('build argv', lambda x: x['argv'].append('-race')),
                ('environment', lambda x: x['environment'].update(GOMAXPROCS='8')),
                ('failed build', lambda x: x.update(exit_code=1)),
                ('bool build exit', lambda x: x.update(exit_code=False))):
            with self.subTest(case=label):
                changed = copy.deepcopy(self.build)
                mutate(changed)
                with self.assertRaises(ValueError):
                    crypto.check_build(changed)

    def test_config_semantic_mutations(self):
        config = self.prepare()
        original = b.decode(config.read_bytes())
        for label, mutate in (
                ('profile', lambda x: x['crypto'].update(profile='other')),
                ('N', lambda x: x['crypto'].update(length=2)),
                ('role', lambda x: x['crypto'].update(role='probe')),
                ('curve', lambda x: x['crypto'].update(curve='other')),
                ('run', lambda x: x['crypto'].update(run_id='b'*32)),
                ('env', lambda x: x['environment'].update(UNEXPECTED='1')),
                ('GOMAXPROCS', lambda x: x['environment'].update(GOMAXPROCS='8')),
                ('selector', lambda x: x['target_argv'].__setitem__(3, '^TestGroth16N2$')),
                ('probe relabel', lambda x: x.update(entrypoint=binding.ENTRYPOINT)),
                ('missing policy', lambda x: x['effective_limits'].pop('wall_seconds')),
                ('memory', lambda x: x['effective_limits'].update(rss_stop_kib=4194304)),
                ('Guardian', lambda x: x['crypto'].update(guardian_seconds=30))):
            with self.subTest(case=label):
                changed = copy.deepcopy(original)
                mutate(changed)
                self.helper.put(config.parent, 'g4.json', changed)
                with self.assertRaises(ValueError):
                    binding.check(config, live=True)
        self.helper.put(config.parent, 'g4.json', original)
        self.assertEqual(binding.check(config, live=True), original)

    def test_wrong_timeout_and_snapshot_rejected(self):
        config = self.prepare()
        original = b.decode(config.read_bytes())
        policy = dict(self.policy, wall_seconds=2)
        self.helper.put(config.parent, 'policy.snapshot.json', policy)
        original['policy'] = dict(bytes=len(b.encoded(policy)), sha256=b.sha(b.encoded(policy)))
        original['effective_limits'] = policy
        self.helper.put(config.parent, 'g4.json', original)
        with self.assertRaisesRegex(ValueError, 'timeout profile'):
            binding.check(config)
        config = self.prepare('source-case')
        self.helper.put(config.parent, 'go-sources/go.mod', {'synthetic': 'wrong source'})
        with self.assertRaisesRegex(ValueError, 'Go snapshot'):
            binding.check(config)

    def test_crypto_supervisor_selection_and_no_fallback(self):
        config = self.prepare()
        m = binding.check(config)
        with mock.patch.object(supervisor.events, 'Server'), mock.patch.object(supervisor, 'Controller'), \
                mock.patch.object(supervisor, '_supervise', return_value='synthetic') as run, \
                mock.patch.object(supervisor.legacy, 'supervise') as legacy:
            self.assertEqual(supervisor.supervise(m['target_argv'], m['paths']['supervised'],
                             entrypoint=crypto.ENTRYPOINT, config=config), 'synthetic')
            self.assertEqual(run.call_args.kwargs['role'], 'pilot')
            self.assertEqual(run.call_args.kwargs['env_overrides'], {'DZKR_MIMCCHAIN_CPU_PILOT': '1'})
            with self.assertRaises(ValueError):
                supervisor.supervise(m['target_argv'], m['paths']['supervised'],
                                     entrypoint=binding.ENTRYPOINT, config=config)
            legacy.assert_not_called()

    def test_guardian_crypto_headroom_fails_before_any_process_action(self):
        from tools.bench import g4_launch
        config = self.prepare()
        with mock.patch.object(g4_launch, 'available', return_value=4*1024**3), \
                mock.patch.object(g4_launch, 'observe_launch') as observe:
            with self.assertRaisesRegex(ValueError, 'resource preflight'):
                g4_launch.launch(config)
            observe.assert_not_called()
        self.assertFalse((config.parent/'attempt.json').exists())

    def test_guardian_timeout_is_profile_bound_without_lifecycle_changes(self):
        import ast
        tree = ast.parse((b.TOOLS/'g4_launch.py').read_text())
        launch = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'launch')
        text = ast.unparse(launch)
        self.assertIn("guardian_seconds = m['crypto']['guardian_seconds'] if binding.is_crypto(m) else 15", text)
        self.assertIn('guardian_seconds * 1000000000', text)
        self.assertIn("outer_seconds=guardian_seconds", text)
        self.assertEqual(sum(isinstance(n, ast.Call) and ast.unparse(n.func) == 'subprocess.Popen'
                             for n in ast.walk(launch)), 1)

    def bundle(self):
        h = self.helper
        base = h.bundle('base')
        config = self.prepare()
        out, m = config.parent, binding.check(config)
        for name in verify.REQUIRED:
            if name in ('g4.json', 'policy.snapshot.json'):
                continue
            dest = out/name
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes((base/name).read_bytes())
        core = h.get(out, 'supervised/inner/result.json')
        started = h.get(out, 'supervised/inner/started.json')
        for item in (core, started):
            item['argv'] = m['target_argv']
            item['limits']['wall_seconds'] = 3.0
        h.put(out, 'supervised/inner/result.json', core)
        h.put(out, 'supervised/inner/started.json', started)
        s = h.get(out, 'supervised/supervisor.json')
        s.update(g4_config_sha256=b.sha(config.read_bytes()), runner_argv=m['runner_argv'], target_argv=m['target_argv'],
                 inner_result=core, inner_primary=core['primary'], inner_cleanup=core['cleanup'],
                 inner_seconds=3.0, barrier_seconds=2.0, outer_seconds=8.0)
        s['safe_environment'].pop('DZKR_MIMCCHAIN_BARRIER_PROBE')
        s['safe_environment']['DZKR_MIMCCHAIN_CPU_PILOT'] = '1'
        h.put(out, 'supervised/supervisor.json', s)
        g = h.get(out, 'guardian.json')
        g['argv'][-1] = str(config)
        g['outer_seconds'] = 15
        for role, argv in (('bootstrap', m['runner_argv']), ('target', m['target_argv'])):
            g['observations'][role]['argv'] = argv
            g['observations'][role]['environment'] = dict(m['environment'], DZKR_MIMCCHAIN_BARRIER_SOCKET=s['ipc']['socket_path'])
        h.put(out, 'guardian.json', g)
        attempt = h.get(out, 'attempt.json')
        attempt['config_sha256'] = b.sha(config.read_bytes())
        h.put(out, 'attempt.json', attempt)
        meta = dict(run_id=m['run_id'], source_id=m['crypto']['source_id'], fixture_id=m['run_id']+'/N8',
                    length=8, profile=crypto.PROFILE, curve='BN254', backend='groth16', constraint_system='R1CS',
                    constraints=2641, internal_variables=2640, instructions=2641, secret_variables=1,
                    public_variables_including_one=2, gomaxprocs=4, go_version='go1.27.1', num_cpu=20,
                    allocation_scope='whole-process Go benchmark timed-window deltas',
                    gc_policy='testing.Benchmark runs GC before each sample',
                    module_versions={'github.com/consensys/gnark': 'v0.16.3', 'github.com/consensys/gnark-crypto': 'v0.21.0'})
        artifacts = {}
        for name in ('circuit.ccs', 'proving.key', 'verifying.key', 'proof.bin', 'public.json'):
            data = b.encoded(dict(length=8, digest='123')) if name == 'public.json' else b'SYNTHETIC-NOT-CRYPTOGRAPHIC-MATERIAL\n'
            path = out/'crypto'/name
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(data)
            artifacts[name] = dict(bytes=len(data), sha256=b.sha(data))
        meta['artifacts'] = artifacts
        footer = {k: meta[k] for k in ('run_id', 'source_id', 'fixture_id', 'length', 'profile')}
        footer.update(complete=True, operations=17, planned_operations=17)
        ledger, samples = [], []
        for i, (name, timed) in enumerate(crypto.PLAN):
            op = dict(operation_id=name, kind=name, run_id=m['run_id'], fixture_id=meta['fixture_id'],
                      length=8, timed=timed, attempt=i+1, start_ns=i*100, end_ns=0,
                      status='running', calls_started=0, calls_completed=0)
            ledger.append(dict(event='start', operation=dict(op)))
            op.update(end_ns=i*100+10, status='rejected' if name == 'wrong_digest_verify' else 'succeeded',
                      calls_started=1, calls_completed=1)
            if timed:
                sample = dict(benchmark_n=1, callbacks=1, calls_started=1, calls_completed=1, duration_ns=5,
                              allocated_bytes=10, allocations=1, ns_per_op=5, bytes_per_op=10, allocs_per_op=1, valid=True)
                op['sample'] = sample
                samples.append(dict(operation_id=name, sample=sample))
            ledger.append(dict(event='end', operation=op))
        h.put(out, 'crypto/fixture.json', meta)
        h.put(out, 'crypto/footer.json', footer)
        for name, rows in (('ledger', ledger), ('samples', samples)):
            (out/'crypto'/(name+'.jsonl')).write_bytes(b''.join((json.dumps(r, sort_keys=True)+'\n').encode() for r in rows))
        h.reseal(out)
        return out

    def test_synthetic_contract_acceptance(self):
        result = verify.audit(self.bundle())
        self.assertTrue(result['final_accepted'], result)
        self.assertEqual(result['crypto']['prove_calls_succeeded'], 2)
        self.assertEqual(result['crypto']['verify_calls_succeeded'], 2)
        self.assertTrue(result['crypto']['wrong_digest_rejected'])

    def test_crypto_semantic_evidence_negatives(self):
        out = self.bundle()
        h = self.helper
        original = {name: (out/name).read_bytes() for name in crypto.OUTPUTS}
        cases = (
            ('setup missing', 'ledger.jsonl', lambda rows: rows.__delitem__(slice(8,10))),
            ('prove count', 'ledger.jsonl', lambda rows: rows[25]['operation'].update(calls_completed=2)),
            ('verify count', 'ledger.jsonl', lambda rows: rows[27]['operation'].update(calls_completed=0)),
            ('negative rejection', 'ledger.jsonl', lambda rows: rows[29]['operation'].update(status='succeeded')),
            ('phase time', 'ledger.jsonl', lambda rows: rows[25]['operation'].pop('sample')),
            ('run ID', 'ledger.jsonl', lambda rows: rows[9]['operation'].update(run_id='b'*32)),
        )
        for label, name, mutate in cases:
            with self.subTest(case=label):
                for n, data in original.items():
                    (out/n).write_bytes(data)
                rows = [json.loads(x) for x in original['crypto/'+name].splitlines()]
                mutate(rows)
                (out/'crypto'/name).write_bytes(b''.join((json.dumps(r)+'\n').encode() for r in rows))
                h.reseal(out)
                result = verify.audit(out)
                self.assertFalse(result['final_accepted'], result)
                self.assertIn('crypto', str(result['errors']))
        for field, value in (('constraints', 661), ('curve', 'other'), ('backend', 'other')):
            with self.subTest(metadata=field):
                for n, data in original.items():
                    (out/n).write_bytes(data)
                meta = json.loads(original['crypto/fixture.json'])
                meta[field] = value
                h.put(out, 'crypto/fixture.json', meta)
                h.reseal(out)
                result = verify.audit(out)
                self.assertFalse(result['final_accepted'], result)
                self.assertIn('crypto metadata:', str(result['errors']))

    def test_existing_resource_exit_coverage_cleanup_gates_apply(self):
        out = self.bundle()
        h = self.helper
        names = ('supervised/inner/result.json', 'supervised/supervisor.json', 'guardian.json', 'supervised/group-rss.jsonl',
                 'supervised/group-summary.json', 'supervised/bootstrap.json', 'supervisor-events.jsonl',
                 'supervised/bootstrap-events.jsonl')
        original = {n: (out/n).read_bytes() for n in names}
        for label in ('resource contradiction', 'nonzero Bootstrap', 'missing workload', 'cleanup'):
            with self.subTest(case=label):
                for n, data in original.items():
                    (out/n).write_bytes(data)
                if label == 'resource contradiction':
                    rows = [json.loads(x) for x in (out/names[3]).read_bytes().splitlines()]
                    rows[1]['mem_available_bytes'] = 1024**3
                    h.refresh_raw(out, rows)
                    core = h.get(out, names[0])
                    core['minimum_memavailable_kib'] = 1048576
                    h.put(out, names[0], core)
                    s = h.get(out, names[1])
                    s.update(inner_result=core, inner_primary=core['primary'], inner_cleanup=core['cleanup'])
                    h.put(out, names[1], s)
                    boot = h.get(out, 'supervised/bootstrap.json')
                    boot['audit'] = verify.group_audit.audit(out/'supervised/group-rss.jsonl', out/'supervised/group-summary.json')
                    h.put(out, 'supervised/bootstrap.json', boot)
                elif label == 'nonzero Bootstrap':
                    s = h.get(out, names[1]);s['cleanup']['runner_exit_code'] = 2;h.put(out, names[1], s)
                elif label == 'missing workload':
                    rows = [json.loads(x) for x in (out/names[3]).read_bytes().splitlines()]
                    h.refresh_raw(out, [rows[0], rows[-1]])
                else:
                    g = h.get(out, names[2]);g['residual_children'] = [999];h.put(out, names[2], g)
                h.reseal(out)
                result = verify.audit(out)
                self.assertFalse(result['final_accepted'], result)
                self.assertFalse(result['evidence_valid'])
                if label == 'resource contradiction':
                    self.assertIn('resource policy: trigger contradicts command_success', str(result['errors']))


if __name__ == '__main__':
    unittest.main()
