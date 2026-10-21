"""G2 frozen policy and local evidence contract; no process creation on import.

Private snapshots resist other UIDs, not a malicious owner/root. SHA-256 detects
inconsistency; it is not attestation. The mutable input is never read after freeze.
"""
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import uuid

REPO = Path(__file__).resolve().parents[2]
TOOLS = Path(__file__).resolve().parent
APPROVED = {
    'run_check.py': 'b31448c0114902b9f4888077e26f6153453c7f413bf73ad58244a31c3cd6b9ce',
    'supervise.py': 'b5ba161156fa133e17bb8df26410db407a63362e0700cc23c4222a03fb16074a',
    'processes.py': '148852d2f84c5be3b056e7692487f9795f716197a76f17ee1eb80edfe4ef4230',
}
SOURCES = (*APPROVED, 'policy_binding.py', 'policy_launch.py', 'policy_verify.py')
FIXED = dict(phase_seconds=dict(compile=30, setup=90, setup_wrong_vk=90, prove=60),
             rss_stop_kib=2097152, available_stop_kib=2097152,
             stop_escalation_kib=1048576, cpu_seconds=[180, 190],
             core_bytes=0, kernel_memory_limit=False, term_grace=.15, kill_grace=.5)
ENVIRONMENT = dict(PATH=os.defpath, LANG='C.UTF-8', PYTHONDONTWRITEBYTECODE='1',
                   GOTOOLCHAIN='local', GOPROXY='off', GOSUMDB='off', GOMAXPROCS='4',
                   DZKR_MIMCCHAIN_BARRIER_PROBE='1', DZKR_MIMCCHAIN_BARRIER_PROBE_MODE='trivial',
                   DZKR_MIMCCHAIN_START_BARRIER='1', DZKR_MIMCCHAIN_BARRIER_TIMEOUT='5s')


class PolicyError(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise PolicyError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def decode(data):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result
    try:
        return json.loads(data.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False, 'non-finite JSON number'))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PolicyError('invalid JSON evidence') from exc


def parse_policy(data):
    require(0 < len(data) <= 16384, 'policy size out of bounds')
    value = decode(data)
    fields = {'schema', 'wall_seconds', 'barrier_seconds', 'outer_seconds', *FIXED}
    require(isinstance(value, dict) and set(value) == fields, 'policy fields mismatch')
    require(type(value['schema']) is int and value['schema'] == 1, 'unsupported policy schema')
    for key, maximum in [('wall_seconds', 210), ('barrier_seconds', 2), ('outer_seconds', 230)]:
        n = value[key]
        require(type(n) in (int, float) and .01 <= n <= maximum and math.isfinite(n),
                'invalid bounded duration: ' + key)
    require(value['barrier_seconds'] <= value['wall_seconds']
            and value['outer_seconds'] >= value['wall_seconds'] + 5, 'inconsistent lifecycle budgets')
    for key, fixed in FIXED.items():
        # JSON canonical comparison also rejects bool-as-int and nested unknown fields.
        require(json.dumps(value[key], sort_keys=True) == json.dumps(fixed, sort_keys=True),
                'unsupported fixed runner limit: ' + key)
    return value


def read_regular(path, limit=2 * 1024 * 1024):
    """No symlink/FIFO following; detect replacement or modification during read."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1, 'regular single-link file required')
        require(before.st_size <= limit, 'evidence file too large')
        with os.fdopen(os.dup(fd), 'rb') as stream:
            data = stream.read(limit + 1)
            stream.seek(0)
            second = stream.read(limit + 1)
        after = os.fstat(fd)
        current = os.stat(path, follow_symlinks=False)
        keys = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        require(all(getattr(before, k) == getattr(after, k) == getattr(current, k) for k in keys)
                and data == second and len(data) == before.st_size, 'file changed while reading')
        return data
    finally:
        os.close(fd)


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()


def write_private(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(os.dup(fd), 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.fchmod(fd, 0o400)
    finally:
        os.close(fd)


def runner_argv(manifest, policy):
    return [manifest['python'], '-B', str(Path(manifest['tool_directory']) / 'run_check.py'),
            '--out', str(Path(manifest['evidence_directory']['path']) / 'supervised/inner'),
            '--label', 'p4b2-rss-barrier', '--cwd', manifest['cwd'],
            '--wall-seconds', str(policy['wall_seconds']), '--term-grace', '.15',
            '--kill-grace', '.5', '--', *manifest['target_argv']]


def prepare(policy_path, out, target_argv, run_id=None):
    """Freeze once. Later changes to the source policy are permitted and irrelevant."""
    policy_bytes = read_regular(policy_path, 16384)
    policy = parse_policy(policy_bytes)
    require(target_argv and all(isinstance(a, str) and a and '\x00' not in a for a in target_argv),
            'nonempty argv strings required')
    require(Path(target_argv[0]).is_absolute() and Path(target_argv[0]).is_file()
            and os.access(target_argv[0], os.X_OK), 'absolute executable required')
    run_id = uuid.uuid4().hex if run_id is None else run_id
    require(isinstance(run_id, str) and re.fullmatch('[0-9a-f]{32}', run_id) is not None, 'invalid run ID')
    sources = {name: read_regular(TOOLS / name) for name in SOURCES}
    require(all(sha(sources[n]) == h for n, h in APPROVED.items()), 'approved source identity changed')
    out = Path(out)
    require(not out.is_symlink(), 'evidence directory symlink')
    require(not (out / 'sources').is_symlink(), 'source directory symlink')
    out = out.resolve()
    require(not out.is_relative_to(REPO), 'evidence must be external')
    out.mkdir(mode=0o700, exist_ok=False)
    os.chmod(out, 0o700)
    st = out.stat()
    snapshot = out / 'policy.snapshot.json'
    write_private(snapshot, policy_bytes)
    require(read_regular(snapshot) == policy_bytes, 'snapshot creation mismatch')
    (out / 'sources').mkdir(mode=0o700)
    for name, data in sources.items():
        write_private(out / 'sources' / name, data)
    manifest = dict(schema=1, run_id=run_id, state='prepared', role='probe',
                    policy=dict(path=snapshot.name, sha256=sha(policy_bytes), bytes=len(policy_bytes)),
                    effective_limits=policy, target_argv=list(target_argv), python=sys.executable,
                    cwd=str(REPO), tool_directory=str(TOOLS), environment=dict(ENVIRONMENT),
                    tool_sources={name: dict(path='sources/' + name, sha256=sha(data), bytes=len(data))
                                  for name, data in sources.items()},
                    launch_intent_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    evidence_directory=dict(path=str(out), device=st.st_dev, inode=st.st_ino, uid=st.st_uid))
    manifest['runner_argv'] = runner_argv(manifest, policy)
    write_private(out / 'manifest.json', encoded(manifest))
    return out


def check_binding(out):
    """Archive-portable local consistency check, independent of the mutable input."""
    out = Path(out)
    require(not out.is_symlink(), 'evidence directory symlink')
    require(not (out / 'sources').is_symlink(), 'source directory symlink')
    m = decode(read_regular(out / 'manifest.json'))
    required = {'schema', 'run_id', 'state', 'role', 'policy', 'effective_limits', 'target_argv',
                'python', 'cwd', 'tool_directory', 'environment', 'tool_sources',
                'launch_intent_utc', 'evidence_directory', 'runner_argv'}
    require(isinstance(m, dict) and set(m) == required, 'manifest fields mismatch')
    require(type(m['schema']) is int and m['schema'] == 1 and m['state'] == 'prepared'
            and m['role'] == 'probe', 'invalid manifest version/state/role')
    require(isinstance(m['run_id'], str) and re.fullmatch('[0-9a-f]{32}', m['run_id']) is not None, 'invalid run ID')
    require(isinstance(m['launch_intent_utc'], str), 'missing launch time')
    try:
        timestamp = datetime.datetime.fromisoformat(m['launch_intent_utc'])
        require(timestamp.utcoffset() is not None, 'unqualified launch time')
    except ValueError as exc:
        raise PolicyError('invalid launch time') from exc
    require(isinstance(m['policy'], dict) and set(m['policy']) == {'path', 'sha256', 'bytes'}
            and m['policy']['path'] == 'policy.snapshot.json', 'invalid snapshot reference')
    raw = read_regular(out / 'policy.snapshot.json', 16384)
    require(type(m['policy']['bytes']) is int and m['policy']['bytes'] == len(raw)
            and m['policy']['sha256'] == sha(raw), 'snapshot identity mismatch')
    policy = parse_policy(raw)
    require(encoded(m['effective_limits']) == encoded(policy), 'manifest policy disagreement')
    require(m['environment'] == ENVIRONMENT, 'environment mismatch')
    ed = m['evidence_directory']
    require(isinstance(ed, dict) and set(ed) == {'path', 'device', 'inode', 'uid'}
            and isinstance(ed['path'], str) and Path(ed['path']).is_absolute()
            and all(type(ed[k]) is int and ed[k] >= 0 for k in ('device', 'inode', 'uid')), 'invalid evidence identity')
    require(all(isinstance(m[k], str) and Path(m[k]).is_absolute() for k in ('python', 'cwd', 'tool_directory')),
            'invalid invocation paths')
    argv = m['target_argv']
    require(isinstance(argv, list) and argv and all(isinstance(a, str) and a and '\x00' not in a for a in argv)
            and Path(argv[0]).is_absolute(), 'invalid target argv')
    require(m['runner_argv'] == runner_argv(m, policy), 'policy/runner argv disagreement')
    require(isinstance(m['tool_sources'], dict) and set(m['tool_sources']) == set(SOURCES), 'missing source identity')
    for name in SOURCES:
        ref = m['tool_sources'][name]
        require(isinstance(ref, dict) and set(ref) == {'path', 'sha256', 'bytes'}
                and ref['path'] == 'sources/' + name, 'invalid source reference')
        data = read_regular(out / ref['path'])
        require(type(ref['bytes']) is int and ref['bytes'] == len(data) and ref['sha256'] == sha(data), 'source identity mismatch')
        if name in APPROVED:
            require(ref['sha256'] == APPROVED[name], 'unapproved source identity')
    return m, policy
