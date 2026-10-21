"""N8-only frozen launch/evidence contract. No Go, proving or process execution."""
from pathlib import Path

from . import policy_binding as b

from .g4_binding import CRYPTO_PROFILE as PROFILE, CRYPTO_ENTRYPOINT as ENTRYPOINT
SELECTOR = '^TestMiMCChainCPUPilot$/^N8$'
GO_SOURCES = (
    'go.mod', 'go.sum',
    *("internal/zk/experimental/mimcchain/" + name for name in (
        'circuit.go', 'reference.go', 'calibration_test.go', 'circuit_test.go',
        'pilot_driver_test.go', 'pilot_driver_contract_test.go',
        'pilot_n2_test.go', 'pilot_start_barrier_test.go')),
)
# Filled from the reviewed source version, not from candidate evidence.
GO_SHA256 = {'go.mod': 'd1698ff37cedff8106302518185552243ebea1f2cacf8f6c691a00457c79c945',
 'go.sum': '4618f38b9bc00f744efe6e2d1a8757c866065ab22c591e943a5e80b9f5b8ae38',
 'internal/zk/experimental/mimcchain/calibration_test.go': 'd5053323bc66decf8e5d1178b1faebba699866066f6ade0051f2bad671ebcd3f',
 'internal/zk/experimental/mimcchain/circuit.go': '9c7f5f940a44b8f19e565471897e877ab91d5b2413ef73aa4d3eed648c336d56',
 'internal/zk/experimental/mimcchain/circuit_test.go': '04f960ce5399982cf803a64918f48881e81e52ac8225be0f47fa88a9226a613c',
 'internal/zk/experimental/mimcchain/pilot_driver_contract_test.go': '0356709aa7ad7d8323550a1971c982bdb5cd456fea9b354253c4206a220e2ce4',
 'internal/zk/experimental/mimcchain/pilot_driver_test.go': '10ef821a1967f844ea0371dc8a66faa0b4f6d9f5b46bf323d5369522b9101632',
 'internal/zk/experimental/mimcchain/pilot_n2_test.go': '167099eba05514b33d68065f981a8849657a26a4e890ba28a9a08bdc522f63ee',
 'internal/zk/experimental/mimcchain/pilot_start_barrier_test.go': '9aa662d9d28e5f65247deee773879849eead0c06e4393309ca512a8f403f6ea9',
 'internal/zk/experimental/mimcchain/reference.go': 'fade347a8a09370f216a730daa868b9d74e5e9ce0b55db70e70b37a06c036f37'}
OUTPUTS = tuple('crypto/' + n for n in (
    'fixture.json', 'footer.json', 'ledger.jsonl', 'samples.jsonl',
    'circuit.ccs', 'proving.key', 'verifying.key', 'proof.bin', 'public.json'))
EXTRA_ARTIFACTS = ('crypto-build.json', 'sources/g4_crypto.py', 'sources/g4_launch.py', 'sources/g4_verify.py',
                   *('go-sources/' + n for n in GO_SOURCES), *OUTPUTS)
PLAN = (
    ('compile', True), ('digest', False), ('witness', True), ('public_witness', False),
    ('setup', True), ('preflight_prove', False), ('preflight_verify', False),
    ('ccs_serialize', False), ('pk_serialize', True), ('vk_serialize', True),
    ('pk_decode', True), ('vk_decode', True), ('prove', True), ('verify', True),
    ('wrong_digest_verify', False), ('proof_serialize', True), ('artifact_io', False),
)


def target_argv(binary):
    return [binary, '-test.v', '-test.run', SELECTOR, '-test.benchtime=1x',
            '-test.count=1', '-test.cpu=4', '-test.timeout=3m']


def build_argv(compiler, binary):
    return [compiler, 'test', '-mod=readonly', '-p', '2', '-c', '-o', binary,
            './internal/zk/experimental/mimcchain']


def check_build(build):
    from .g4_binding import executable
    b.require(isinstance(build, dict) and set(build) == {
        'schema', 'profile', 'sources', 'binary', 'toolchain', 'go_version',
        'argv', 'cwd', 'environment', 'exit_code'}, 'crypto build fields')
    b.require(type(build['schema']) is int and build['schema'] == 1
              and build['profile'] == PROFILE, 'crypto build version')
    b.require(type(build['exit_code']) is int and build['exit_code'] == 0, 'crypto build exit')
    b.require(build['go_version'] == 'go1.27.1', 'crypto Go version')
    b.require(isinstance(build['sources'], dict) and set(build['sources']) == set(GO_SOURCES),
              'crypto Go source set')
    for name in GO_SOURCES:
        raw = b.read_regular(b.REPO / name)
        expected = dict(bytes=len(raw), sha256=b.sha(raw))
        b.require(expected['sha256'] == GO_SHA256[name]
                  and b.encoded(build['sources'][name]) == b.encoded(expected), 'crypto Go source: ' + name)
    for name in ('binary', 'toolchain'):
        ref = build[name]
        b.require(isinstance(ref, dict) and isinstance(ref.get('path'), str)
                  and b.encoded(ref) == b.encoded(executable(ref['path'])), 'crypto executable: ' + name)
    version = Path(build['toolchain']['path']).parent.parent / 'VERSION'
    b.require(b.read_regular(version).decode().splitlines()[0] == build['go_version'], 'crypto toolchain VERSION')
    b.require(build['cwd'] == str(b.REPO), 'crypto build cwd')
    b.require(build['argv'] == build_argv(build['toolchain']['path'], build['binary']['path']),
              'crypto build argv')
    b.require(build['environment'] == dict(GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOMAXPROCS='4'),
              'crypto build environment')


def profile(out, run_id, build_raw):
    build = b.decode(build_raw)
    check_build(build)
    return dict(profile=PROFILE, role='pilot', length=8, curve='BN254', backend='groth16',
                constraint_system='R1CS', constraints=2641, source_id=b.sha(build_raw),
                output=str(Path(out) / 'crypto'), run_id=run_id, guardian_seconds=15,
                outer_seconds=25, minimum_available_bytes=5 * 1024**3,
                maximum_targets=1, maximum_attempts=1)


def environment(m):
    env = {k: v for k, v in b.ENVIRONMENT.items() if not k.startswith('DZKR_MIMCCHAIN_BARRIER_PROBE')}
    env.update(GOGC='100', DZKR_MIMCCHAIN_CPU_PILOT='1', DZKR_MIMCCHAIN_CPU_N='8',
               DZKR_MIMCCHAIN_EVIDENCE_DIR=m['crypto']['output'], DZKR_MIMCCHAIN_RUN_ID=m['run_id'],
               DZKR_MIMCCHAIN_SOURCE_ID=m['crypto']['source_id'])
    return env


def check(m, out):
    raw = b.read_regular(Path(out) / 'crypto-build.json')
    b.require(b.encoded(m['crypto']) == b.encoded(profile(out, m['run_id'], raw)), 'crypto profile identity')
    build = b.decode(raw)
    b.require(m['target_argv'] == target_argv(build['binary']['path'])
              and m['target_executable'] == build['binary'], 'crypto target argv/executable')
    b.require(m['environment'] == environment(m), 'crypto environment')
    b.require([m['effective_limits'][k] for k in ('wall_seconds', 'barrier_seconds', 'outer_seconds')]
              == [3, 2, 8], 'crypto timeout profile')
    for name in GO_SOURCES:
        data = b.read_regular(Path(out) / 'go-sources' / name)
        b.require(dict(bytes=len(data), sha256=b.sha(data)) == build['sources'][name],
                  'crypto Go snapshot: ' + name)


def evidence(raw, m, state=None):
    """Reconcile recorded bound Go calls, NOT an independent Groth16 verification."""
    require = b.require
    state = {} if state is None else state
    load = lambda name: b.decode(raw['crypto/' + name])
    meta, footer = load('fixture.json'), load('footer.json')
    ledger = [b.decode(x) for x in raw['crypto/ledger.jsonl'].splitlines()]
    samples = [b.decode(x) for x in raw['crypto/samples.jsonl'].splitlines()]
    identity = dict(run_id=m['run_id'], source_id=m['crypto']['source_id'],
                    fixture_id=m['run_id'] + '/N8', length=8, profile=PROFILE)
    for obj in (meta, footer):
        require(isinstance(obj, dict) and all(b.encoded(obj.get(k)) == b.encoded(v) for k, v in identity.items()), 'crypto record identity')
    for k, v in dict(curve='BN254', backend='groth16', constraint_system='R1CS', constraints=2641,
                     internal_variables=2640, instructions=2641, secret_variables=1,
                     public_variables_including_one=2, gomaxprocs=4, go_version='go1.27.1',
                     module_versions={'github.com/consensys/gnark': 'v0.16.3',
                                      'github.com/consensys/gnark-crypto': 'v0.21.0'}).items():
        require(b.encoded(meta.get(k)) == b.encoded(v), 'crypto metadata: ' + k)
    require(type(meta.get('num_cpu')) is int and meta['num_cpu'] > 0
            and meta.get('allocation_scope') == 'whole-process Go benchmark timed-window deltas'
            and meta.get('gc_policy') == 'testing.Benchmark runs GC before each sample', 'crypto timing contract')
    state['attempted'] = bool(ledger)
    require(footer.get('complete') is True and type(footer.get('operations')) is int
            and footer['operations'] == len(PLAN) and type(footer.get('planned_operations')) is int
            and footer['planned_operations'] == len(PLAN), 'crypto footer completeness')
    require(len(ledger) == 2 * len(PLAN), 'crypto operation count')
    timed, last = [], -1
    for i, (name, measured) in enumerate(PLAN):
        a, c = ledger[2*i:2*i+2]
        require(a.get('event') == 'start' and c.get('event') == 'end', 'crypto operation event')
        a, c = a['operation'], c['operation']
        expected = dict(operation_id=name, kind=name, run_id=m['run_id'], length=8,
                        timed=measured, attempt=i+1, fixture_id=identity['fixture_id'])
        for op in (a, c):
            require(all(b.encoded(op.get(k)) == b.encoded(v) for k, v in expected.items()), 'crypto operation identity/order')
        require(type(a.get('start_ns')) is int and type(c.get('end_ns')) is int and type(c.get('start_ns')) is int
                and last <= a['start_ns'] <= c['end_ns'] and c.get('start_ns') == a['start_ns'], 'crypto operation timing')
        last = c['end_ns']
        require(a.get('status') == 'running' and type(a.get('calls_started')) is int
                and a['calls_started'] == 0 and type(a.get('calls_completed')) is int and a['calls_completed'] == 0,
                'crypto operation start count')
        require(c.get('status') == ('rejected' if name == 'wrong_digest_verify' else 'succeeded')
                and not c.get('error_category') and type(c.get('calls_started')) is int and c['calls_started'] == 1
                and type(c.get('calls_completed')) is int and c['calls_completed'] == 1, 'crypto operation outcome/count')
        if name == 'setup':
            state['setup_succeeded'] = True
        elif name in ('preflight_prove', 'prove'):
            state['prove_calls_succeeded'] = (state.get('prove_calls_succeeded') or 0) + 1
        elif name in ('preflight_verify', 'verify'):
            state['verify_calls_succeeded'] = (state.get('verify_calls_succeeded') or 0) + 1
        elif name == 'wrong_digest_verify':
            state['wrong_digest_rejected'] = True
        if measured:
            s = c.get('sample')
            require(isinstance(s, dict) and s.get('valid') is True, 'crypto missing phase sample')
            for k in ('benchmark_n', 'callbacks', 'calls_started', 'calls_completed'):
                require(type(s.get(k)) is int and s[k] == 1, 'crypto benchmark count')
            for total, per_op in (('duration_ns', 'ns_per_op'), ('allocated_bytes', 'bytes_per_op'), ('allocations', 'allocs_per_op')):
                require(type(s.get(total)) is int and type(s.get(per_op)) is int
                        and s[total] >= 0 and s[total] == s[per_op], 'crypto sample arithmetic')
            require(0 < s['duration_ns'] <= c['end_ns']-a['start_ns'], 'crypto sample timing')
            timed.append(dict(operation_id=name, sample=s))
        else:
            require(c.get('sample') is None, 'crypto unexpected phase sample')
    require(b.encoded(samples) == b.encoded(timed), 'crypto phase sample copies')
    artifact_names = ('circuit.ccs', 'proving.key', 'verifying.key', 'proof.bin', 'public.json')
    require(isinstance(meta.get('artifacts'), dict) and set(meta['artifacts']) == set(artifact_names), 'crypto artifact set')
    for name in artifact_names:
        data = raw['crypto/' + name]
        require(data and b.encoded(meta['artifacts'][name]) == b.encoded(dict(bytes=len(data), sha256=b.sha(data))),
                'crypto artifact identity')
    statement = load('public.json')
    require(isinstance(statement, dict) and set(statement) == {'length', 'digest'} and type(statement['length']) is int
            and statement['length'] == 8 and isinstance(statement['digest'], str)
            and statement['digest'].isascii() and statement['digest'].isdigit(), 'crypto public statement')
    state.update(attempted=True, setup_succeeded=True, prove_calls_succeeded=2, verify_calls_succeeded=2,
                wrong_digest_rejected=True, timing_evidence_present=True, evidence_valid=True,
                verification_scope='recorded calls in source-bound Go target; no Python Groth16 Verify')
    return state
