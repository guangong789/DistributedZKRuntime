"""G4 interface v1: frozen launch intent, separate from the unchanged G2 v1 contract."""
import os
from pathlib import Path
import re
import sys

from . import policy_binding as b

CONTRACT = 'g4-runner-interface-v1'
ENTRYPOINT = 'g4-bootstrap-v1'
CRYPTO_PROFILE = 'mimcchain-n8-groth16-v1'
CRYPTO_ENTRYPOINT = 'g4-bootstrap-crypto-v1'
PINNED = dict(b.APPROVED, **{
    'group_rss.py': 'de2c3c11bd55e3a3160b3db695db4fa72170352f2ca1221a612d10bfc065abef',
    'group_schema.py': 'c36fce5bfde0e0bda86ee448cb4e273e5f05625134c17583ae4cf278f7bee998',
    'group_audit.py': 'a73481e3210984eb17cd492872392b04e5561ccad4284c99b43a277e11b1552a',
})
SOURCES = (*PINNED, '__init__.py', 'policy_binding.py', 'g4_binding.py', 'g4_events.py',
           'g4_bootstrap.py', 'g4_supervise.py')
FIELDS = {'schema', 'contract', 'entrypoint', 'run_id', 'policy', 'effective_limits',
          'sources', 'python', 'cwd', 'environment', 'target_argv', 'target_executable',
          'paths', 'directory', 'runner_argv'}
require = b.require


def run_id(value):
    require(isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None,
            'invalid G4 run ID')
    return value


def executable(path):
    require(isinstance(path, str) and Path(path).is_absolute(), 'absolute executable required')
    resolved = Path(path).resolve(strict=True)
    data = b.read_regular(resolved, 64 * 1024 * 1024)
    st = resolved.stat()
    require(os.access(resolved, os.X_OK), 'executable permission required')
    return dict(path=str(resolved), device=st.st_dev, inode=st.st_ino,
                bytes=len(data), sha256=b.sha(data))


def paths(out, identity):
    return dict(config=str(out / 'g4.json'), out=str(out),
                supervised=str(out / 'supervised'), inner=str(out / 'supervised/inner'),
                raw=str(out / 'supervised/group-rss.jsonl'),
                summary=str(out / 'supervised/group-summary.json'),
                channel='/tmp/dzkr-g4-' + identity + '/events.sock')


def runner_argv(m):
    return [m['python']['path'], '-B', '-m', 'tools.bench.g4_bootstrap',
            '--config', m['paths']['config']]


def is_crypto(m):
    return m.get('schema') == 2


def source_names(m):
    return (*SOURCES, 'g4_crypto.py', 'g4_launch.py', 'g4_verify.py') if is_crypto(m) else SOURCES


def launch_role(m):
    return m['crypto']['role'] if is_crypto(m) else 'probe'


def launch_overrides(m):
    return {'DZKR_MIMCCHAIN_CPU_PILOT': '1'} if is_crypto(m) else {'DZKR_MIMCCHAIN_BARRIER_PROBE': '1'}


def prepare(policy_path, out, target_argv, identity, *, profile=None, build_manifest=None):
    """Freeze before launch; the caller supplies the single authoritative run ID."""
    require(profile in (None, CRYPTO_PROFILE), 'unknown G4 profile')
    if profile is not None:
        from . import g4_crypto
    require((profile is None) == (build_manifest is None), 'explicit crypto build/profile required')
    build_raw = b.read_regular(build_manifest) if build_manifest is not None else None
    if build_raw is not None:
        g4_crypto.check_build(b.decode(build_raw))
    run_id(identity)
    raw = b.read_regular(policy_path, 16384)
    policy = b.parse_policy(raw)
    require(isinstance(target_argv, list) and target_argv
            and all(isinstance(x, str) and x and '\x00' not in x for x in target_argv),
            'invalid G4 target argv')
    if profile:
        require(target_argv == g4_crypto.target_argv(b.decode(build_raw)['binary']['path'])
                and [policy[k] for k in ('wall_seconds', 'barrier_seconds', 'outer_seconds')] == [3, 2, 8],
                'crypto argv/timeout profile')
    target = executable(target_argv[0])
    names = (*SOURCES, 'g4_crypto.py', 'g4_launch.py', 'g4_verify.py') if profile else SOURCES
    sources = {n: b.read_regular(b.TOOLS / n) for n in names}
    require(all(b.sha(sources[n]) == h for n, h in PINNED.items()), 'unapproved G4 dependency')
    out = Path(out)
    require(not out.is_symlink(), 'G4 evidence directory symlink')
    out = out.resolve()
    require(not out.is_relative_to(b.REPO), 'G4 evidence must be external')
    out.mkdir(mode=0o700, exist_ok=False)
    os.chmod(out, 0o700)
    st = out.stat()
    (out / 'sources').mkdir(mode=0o700)
    b.write_private(out / 'policy.snapshot.json', raw)
    for n, data in sources.items():
        b.write_private(out / 'sources' / n, data)
    m = dict(schema=1, contract=CONTRACT, entrypoint=ENTRYPOINT, run_id=identity,
             policy=dict(bytes=len(raw), sha256=b.sha(raw)), effective_limits=policy,
             sources={n: dict(bytes=len(data), sha256=b.sha(data)) for n, data in sources.items()},
             python=executable(sys.executable), cwd=str(b.REPO), environment=dict(b.ENVIRONMENT),
             target_argv=list(target_argv), target_executable=target, paths=paths(out, identity),
             directory=dict(device=st.st_dev, inode=st.st_ino, uid=st.st_uid))
    if profile:
        m.update(schema=2, entrypoint=g4_crypto.ENTRYPOINT)
        m['crypto'] = g4_crypto.profile(out, identity, build_raw)
        m['environment'] = g4_crypto.environment(m)
        b.write_private(out / 'crypto-build.json', build_raw)
        for name in g4_crypto.GO_SOURCES:
            path = out / 'go-sources' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            b.write_private(path, b.read_regular(b.REPO / name))
    m['runner_argv'] = runner_argv(m)
    b.write_private(out / 'g4.json', b.encoded(m))
    return out / 'g4.json'


def check(config, *, live=False):
    """Validate frozen evidence; live additionally verifies this installation and target."""
    config = Path(config)
    require(config.name == 'g4.json' and not config.is_symlink(), 'G4 config path')
    out = config.parent
    require(not out.is_symlink() and not (out / 'sources').is_symlink(), 'G4 directory symlink')
    m = b.decode(b.read_regular(config))
    require(isinstance(m, dict), 'G4 manifest fields')
    crypto = is_crypto(m)
    require(set(m) == FIELDS | ({'crypto'} if crypto else set()), 'G4 manifest fields')
    require(type(m['schema']) is int and m['schema'] in (1, 2) and m['contract'] == CONTRACT
            and m['entrypoint'] == (CRYPTO_ENTRYPOINT if crypto else ENTRYPOINT),
            'unapproved G4 entrypoint/version')
    run_id(m['run_id'])
    raw = b.read_regular(out / 'policy.snapshot.json', 16384)
    require(b.encoded(m['policy']) == b.encoded(dict(bytes=len(raw), sha256=b.sha(raw))),
            'G4 policy snapshot identity')
    policy = b.parse_policy(raw)
    require(b.encoded(m['effective_limits']) == b.encoded(policy), 'G4 effective policy mismatch')
    if crypto:
        from . import g4_crypto
        g4_crypto.check(m, out)
    else:
        require(m['environment'] == b.ENVIRONMENT, 'G4 environment mismatch')
    require(isinstance(m['paths'], dict) and isinstance(m['paths'].get('out'), str)
            and Path(m['paths']['out']).is_absolute(), 'G4 output paths')
    require(m['paths'] == paths(Path(m['paths']['out']), m['run_id']), 'G4 paths mismatch')
    require(isinstance(m['directory'], dict) and set(m['directory']) == {'device', 'inode', 'uid'}
            and all(type(v) is int and v >= 0 for v in m['directory'].values()), 'G4 directory identity')
    require(isinstance(m['python'], dict) and set(m['python']) == {'path', 'bytes', 'sha256', 'device', 'inode'}
            and isinstance(m['python']['path'], str) and Path(m['python']['path']).is_absolute(),
            'G4 Python identity')
    require(isinstance(m['cwd'], str) and Path(m['cwd']).is_absolute(), 'G4 cwd')
    require(isinstance(m['target_argv'], list) and m['target_argv']
            and all(isinstance(x, str) and x and '\x00' not in x for x in m['target_argv'])
            and Path(m['target_argv'][0]).is_absolute(), 'G4 target argv')
    require(isinstance(m['target_executable'], dict)
            and set(m['target_executable']) == {'path', 'bytes', 'sha256', 'device', 'inode'},
            'G4 target identity')
    for ref in (m['python'], m['target_executable']):
        require(isinstance(ref['path'], str) and Path(ref['path']).is_absolute()
                and all(type(ref[k]) is int and ref[k] >= 0 for k in ('bytes', 'device', 'inode'))
                and isinstance(ref['sha256'], str) and re.fullmatch('[0-9a-f]{64}', ref['sha256']),
                'G4 executable fields')
    require(m['runner_argv'] == runner_argv(m), 'G4 actual argv mismatch')
    require(isinstance(m['sources'], dict) and set(m['sources']) == set(source_names(m)), 'G4 source set')
    for name in source_names(m):
        data = b.read_regular(out / 'sources' / name)
        require(b.encoded(m['sources'][name]) == b.encoded(dict(bytes=len(data), sha256=b.sha(data))),
                'G4 source identity mismatch')
        if name in PINNED:
            require(b.sha(data) == PINNED[name], 'G4 pinned source mismatch')
        if live:
            require(b.read_regular(b.TOOLS / name) == data, 'G4 live source mismatch')
    if live:
        st = out.stat()
        require(m['paths']['config'] == str(config.absolute()) and m['paths']['out'] == str(out.absolute())
                and st.st_uid == os.getuid() and st.st_mode & 0o077 == 0
                and m['directory'] == dict(device=st.st_dev, inode=st.st_ino, uid=st.st_uid),
                'G4 live directory mismatch')
        require(m['cwd'] == str(b.REPO) and m['python'] == executable(sys.executable)
                and m['target_executable'] == executable(m['target_argv'][0]), 'G4 live executable mismatch')
    return m
