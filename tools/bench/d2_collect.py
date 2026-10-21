"""D2-only call adapter for the accepted Guardian and G1 owned-child cleanup.

No work on import. prepare freezes intent; run requires separate N8 authorization.
"""
import argparse
import ctypes
import fcntl
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import sys
import time

from . import g4_binding as binding, policy_binding as b, processes as ps
from .tests import harness

CONTRACT = 'd2-outer-collector-v1'
D2_PARENT = b.REPO.parent / 'benchmark-archives/DistributedZKRuntime/p4-d2'
EXTRA = ('d2_collect.py', 'tests/harness.py', 'tests/__init__.py')
PINS = {'tests/harness.py': 'da5982d451cf78822b7466d4b04ebf804dad305cc92b5b7bc71ff95246a22cad',
        'tests/__init__.py': '32f7f30f7864dfd23b4ea2506a784ace2e5c62718eef3d232e81d8c0c5573b1d'}
VERIFY = '''import json,sys
from pathlib import Path
from tools.bench import g4_verify as v
out=Path(sys.argv[1])
v.seal(out)
r=v.audit(out)
print(json.dumps(r,sort_keys=True))
sys.exit(0 if r['final_accepted'] else 1 if r['evidence_valid'] else 2)
'''


def commands(m):
    return dict(guardian=[m['python']['path'], '-B', '-m', 'tools.bench.g4_launch',
                          '--config', m['paths']['config']],
                verification=[m['python']['path'], '-B', '-c', VERIFY, m['paths']['out']])


def contract(config):
    m = binding.check(config, live=True)
    out = Path(m['paths']['out'])
    b.require(binding.is_crypto(m) and m['entrypoint'] == binding.CRYPTO_ENTRYPOINT,
              'D2 requires crypto profile')
    b.require(out.name == 'g4' and out.parent.parent == D2_PARENT
              and re.fullmatch(r'n8-cpu-pilot-[0-9]{8}T[0-9]{6}Z', out.parent.name), 'D2 directory')
    sources = {n: b.read_regular(b.TOOLS / n) for n in EXTRA}
    b.require(all(b.sha(sources[n]) == h for n, h in PINS.items()), 'D2 cleanup source')
    intent = dict(contract=CONTRACT, run_id=m['run_id'], config_sha256=b.sha(b.read_regular(config)),
                  directory=m['directory'], policy=m['policy'], limits=m['effective_limits'],
                  crypto=m['crypto'], python=m['python'], target=m['target_executable'],
                  cwd=m['cwd'], environment=m['environment'], commands=commands(m),
                  adapter_argv=[m['python']['path'], '-B', '-m', 'tools.bench.d2_collect',
                                'run', '--config', m['paths']['config']],
                  outer_seconds=25, maximum_descendants=8, disk_minimum_bytes=2*1024**3,
                  minimum_available_bytes=5*1024**3,
                  sources={n: dict(bytes=len(raw), sha256=b.sha(raw)) for n, raw in sources.items()})
    b.require(m['crypto']['outer_seconds'] == intent['outer_seconds'], 'D2 outer budget')
    return m, intent, sources


def fresh(out):
    for n in ('attempt.json', 'outer-attempt.json', 'outer-result.json', 'supervised', 'crypto',
              'guardian.json', 'g4-artifacts.json', 'outer-guardian.stdout', 'outer-guardian.stderr',
              'outer-verification.stdout', 'outer-verification.stderr'):
        b.require(not (out/n).exists() and not (out/n).is_symlink(), 'D2 attempt already consumed')


def prepare(config):
    m, intent, sources = contract(config)
    out = Path(m['paths']['out'])
    fresh(out)
    root = out/'outer-sources'
    root.mkdir(mode=0o700, exist_ok=False)
    for n, raw in sources.items():
        p = root/n
        p.parent.mkdir(parents=True, exist_ok=True)
        b.write_private(p, raw)
    b.write_private(out/'outer-intent.json', b.encoded(intent))
    return intent


def checked(config):
    m, intent, sources = contract(config)
    out = Path(m['paths']['out'])
    b.require(b.encoded(b.decode(b.read_regular(out/'outer-intent.json'))) == b.encoded(intent),
              'D2 frozen intent mismatch')
    for n, raw in sources.items():
        b.require(b.read_regular(out/'outer-sources'/n) == raw, 'D2 source snapshot')
    return m, intent


def available():
    return next(int(x.split()[1])*1024 for x in Path('/proc/meminfo').read_text().splitlines()
                if x.startswith('MemAvailable:'))


def competing(target):
    """Read-only eligibility scan, never authority for signaling."""
    found = []
    for p in Path('/proc').iterdir():
        if not p.name.isdigit() or int(p.name) == os.getpid():
            continue
        try:
            argv = p.joinpath('cmdline').read_bytes().rstrip(b'\0').split(b'\0')
            words = [x.decode(errors='replace') for x in argv]
            if any(x in ('tools.bench.g4_launch', 'tools.bench.g4_bootstrap', target)
                   or x.startswith('/') and Path(x).name == 'mimcchain.test' for x in words):
                found.append(int(p.name))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return found


def preflight(intent):
    b.require(sys.platform == 'linux' and hasattr(os, 'WNOWAIT') and hasattr(os, 'pidfd_open')
              and hasattr(signal, 'pidfd_send_signal'), 'D2 Linux ownership primitives')
    b.require(not harness.children(os.getpid()), 'D2 must start without children')
    fd = os.pidfd_open(os.getpid())  # Permission/kernel probe before consuming an attempt.
    os.close(fd)
    snapshot = dict(memavailable_bytes=available(), disk_available_bytes=os.statvfs(D2_PARENT).f_bavail
                    * os.statvfs(D2_PARENT).f_frsize, load1=os.getloadavg()[0],
                    cpus=len(os.sched_getaffinity(0)), competing=competing(intent['target']['path']))
    b.require(snapshot['memavailable_bytes'] >= intent['minimum_available_bytes']
              and snapshot['disk_available_bytes'] >= intent['disk_minimum_bytes']
              and snapshot['cpus'] >= 4 and snapshot['load1'] < snapshot['cpus']
              and not snapshot['competing'], 'D2 resource/competing preflight')
    return snapshot


def lock():
    fd = os.open(D2_PARENT/'.n8-outer.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        st = os.fstat(fd)
        b.require(stat.S_ISREG(st.st_mode) and st.st_nlink == 1 and st.st_uid == os.getuid(), 'D2 lock file')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def child(argv, phase, m, deadline, pending):
    """One directly created child; only accepted Harness code may signal/reap it."""
    out = Path(m['paths']['out'])
    r = dict(argv=argv, owner=None, waitid=None, raw_exit_code=None, signal=None, stop=None,
             exited_before_deadline=False, cleanup_complete=False, residual_children=None,
             signals=[], errors=[], start_ns=time.monotonic_ns())
    proc, pidfd = None, None
    try:
        with (out/('outer-'+phase+'.stdout')).open('xb') as stdout, (out/('outer-'+phase+'.stderr')).open('xb') as stderr:
            proc = subprocess.Popen(argv, cwd=m['cwd'], env=dict(m['environment']), stdout=stdout,
                                    stderr=stderr, start_new_session=True)
            pidfd = os.pidfd_open(proc.pid)
            owner = ps.identity(proc.pid)
            r['owner'] = owner
            b.require(owner and owner['ppid'] == os.getpid() and owner['pid'] == owner['pgid'] == owner['sid'],
                      'D2 child ownership')
            if owner['state'] not in ('Z', 'X'):
                image = ps.executable(proc.pid)
                b.require(all(image[k] == m['python'][k] for k in ('path', 'device', 'inode', 'sha256'))
                          and ps.same(owner, ps.identity(proc.pid)), 'D2 child executable/generation')
            while True:
                if time.monotonic_ns() >= deadline:
                    r['stop'] = 'outer_timeout'
                    break
                if pending:
                    r['stop'] = 'collector_interrupted'
                    break
                event = os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if event is not None:
                    r['waitid'] = dict(code=event.si_code, status=event.si_status)
                    r['exited_before_deadline'] = time.monotonic_ns() < deadline
                    if not r['exited_before_deadline']:
                        r['stop'] = 'outer_timeout'
                    if any(p != proc.pid for p in harness.children(os.getpid())):
                        r['stop'] = r['stop'] or 'unexpected_descendants'
                    break
                harness.reap_adopted(proc.pid)
                if len(harness.descendants(os.getpid())) > 8 or available() < 2*1024**3:
                    r['stop'] = 'outer_resource_stop'
                    break
                select.select([pidfd], [], [], min(.01, max(0, (deadline-time.monotonic_ns())/1e9)))
    except BaseException as exc:
        r['stop'] = r['stop'] or 'collector_exception'
        r['errors'].append(type(exc).__name__)
    finally:
        if proc is not None:
            try:
                r['residual_children'] = harness.cleanup(proc, r['signals'])
                r['raw_exit_code'] = proc.returncode
                r['signal'] = -proc.returncode if proc.returncode is not None and proc.returncode < 0 else None
                r['cleanup_complete'] = r['residual_children'] == [] and type(proc.returncode) is int
            except BaseException as exc:
                r['errors'].append('cleanup:' + type(exc).__name__)
            # Even a failed cleanup must not erase an already observed child exit.
            r['raw_exit_code'] = proc.returncode
            r['signal'] = -proc.returncode if type(proc.returncode) is int and proc.returncode < 0 else None
        if pidfd is not None:
            try:
                os.close(pidfd)
            except OSError as exc:
                r['stop'] = r['stop'] or 'collector_exception'
                r['errors'].append('pidfd_close:' + type(exc).__name__)
        r['end_ns'] = time.monotonic_ns()
    return r


def failures(out, audit):
    """Diagnostic labels only; the accepted validator alone judges evidence."""
    labels = []
    if not (out/'guardian.json').is_file():
        labels.append('missing_guardian_result')
    if not (out/'g4-artifacts.json').is_file():
        labels.append('missing_g4_evidence')
    if not (out/'crypto/ledger.jsonl').is_file():
        labels.append('missing_crypto_ledger')
    for n in ('guardian.json', 'supervised/supervisor.json', 'supervised/inner/result.json'):
        try:
            value = b.decode(b.read_regular(out/n))
            text = str(value.get('stop', '')) + str(value.get('reason', '')) + str(value.get('primary', ''))
            for word, label in (('resource', 'resource_stop'), ('headroom', 'resource_stop'),
                                ('barrier', 'barrier_failure'), ('timeout', 'guardian_or_execution_timeout')):
                if word in text:
                    labels.append(label)
        except (OSError, ValueError, TypeError, AttributeError):
            pass
    if audit is not None and audit.get('final_accepted') is not True:
        labels.append('evidence_rejected')
        if isinstance(audit.get('crypto'), dict) and audit['crypto'].get('evidence_valid') is not True:
            labels.append('crypto_evidence_rejected')
        # Preserve the accepted validator's complete report as well as broad diagnostic labels.
        text = str(audit.get('errors', []))
        for word, label in (('artifact', 'artifact_corruption'), ('binding', 'source_policy_binding_failure'),
                            ('source', 'source_policy_binding_failure'), ('crypto', 'crypto_evidence_rejected')):
            if word in text:
                labels.append(label)
    return sorted(set(labels))


def collect(config):
    m, intent = checked(config)
    out = Path(m['paths']['out'])
    fresh(out)
    b.require(os.getcwd() == m['cwd'] and binding.executable(sys.executable) == m['python'], 'D2 runtime cwd/Python')
    b.require(sys.orig_argv == intent['adapter_argv'] and not sys.flags.optimize, 'D2 adapter argv')
    b.require(dict(os.environ) == intent['environment'], 'D2 adapter environment')
    lockfd = lock()
    pending, old, result, stage = [], {}, None, 'preflight'
    try:
        snapshot = preflight(intent)
        b.require(not Path(m['paths']['channel']).parent.exists(), 'D2 stale IPC')
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), 'D2 subreaper unavailable')
        checked(config)  # Recheck after eligibility, before reserving the only launch.
        start = time.monotonic_ns()
        deadline = start + intent['outer_seconds']*1_000_000_000
        b.write_private(out/'outer-attempt.json', b.encoded(dict(contract=CONTRACT, run_id=m['run_id'],
                        config_sha256=intent['config_sha256'], intent_sha256=b.sha(b.encoded(intent)),
                        start_ns=start, deadline_ns=deadline,
                        owner=ps.identity(os.getpid()))))
        result = dict(contract=CONTRACT, run_id=m['run_id'], config_sha256=intent['config_sha256'],
                      intent_sha256=b.sha(b.encoded(intent)),
                      start_ns=start, deadline_ns=deadline, preflight=snapshot, phases={},
                      primary_failure=None, failures=[], collector_errors=[], outer_timed_out=False,
                      source_binding_valid=False, artifact_integrity_valid=False,
                      g4_evidence_accepted=False, crypto_evidence_accepted=False,
                      cleanup_complete=False, verification_report=None, accepted=False)
        for number in (signal.SIGTERM, signal.SIGINT):
            old[number] = signal.signal(number, lambda n, frame: pending.append(n))
        for phase, argv in intent['commands'].items():
            if time.monotonic_ns() >= deadline or pending:
                result['outer_timed_out'] = time.monotonic_ns() >= deadline
                result['primary_failure'] = result['primary_failure'] or ('outer_timeout' if result['outer_timed_out'] else 'collector_interrupted')
                break
            stage = 'source_policy_binding_failure'
            checked(config)
            stage = phase
            row = child(argv, phase, m, deadline, pending)
            result['phases'][phase] = row
            if row['stop'] == 'outer_timeout' or time.monotonic_ns() >= deadline:
                result['outer_timed_out'] = True
            reason = row['stop'] or ('cleanup_incomplete' if not row['cleanup_complete'] else
                                    'cleanup_error' if row['errors'] else
                                    'emergency_cleanup' if row.get('signals') else
                                    'child_nonzero' if type(row['raw_exit_code']) is not int or row['raw_exit_code'] != 0 else None)
            if reason:
                result['failures'].append(dict(phase=phase, reason=reason))
                result['primary_failure'] = result['primary_failure'] or reason
            if result['outer_timed_out'] or not row['cleanup_complete']:
                break
        audit = None
        verification = result['phases'].get('verification', {})
        if 'verification' in result['phases'] and (out/'outer-verification.stdout').is_file():
            stage = 'invalid_verification_report'
            try:
                audit = b.decode(b.read_regular(out/'outer-verification.stdout'))
                b.require(isinstance(audit, dict), 'D2 verification report')
            except (OSError, ValueError, TypeError):
                audit = None
                result['failures'].append(dict(phase='verification', reason=stage))
            result['verification_report'] = audit
            if audit is not None and type(verification.get('raw_exit_code')) is int and verification['raw_exit_code'] == 0:
                stage = 'verification_run_id_mismatch'
                b.require(audit.get('run_id') == m['run_id'], 'D2 verification run ID')
                result['g4_evidence_accepted'] = audit.get('final_accepted') is True
                result['crypto_evidence_accepted'] = isinstance(audit.get('crypto'), dict) and audit['crypto'].get('evidence_valid') is True
                result['artifact_integrity_valid'] = audit.get('evidence_valid') is True
        stage = 'source_policy_binding_failure'
        checked(config)
        stage = 'collector_exception'
        result['source_binding_valid'] = True
        result['failures'] += [dict(phase='evidence', reason=x) for x in failures(out, audit)]
        result['cleanup_complete'] = not harness.children(os.getpid()) and bool(result['phases']) and all(x['cleanup_complete'] for x in result['phases'].values())
        guardian = result['phases'].get('guardian', {})
        guardian_ok = guardian.get('raw_exit_code') == 0 and guardian.get('stop') is None and guardian.get('exited_before_deadline') is True
        result['outer_timed_out'] |= time.monotonic_ns() >= deadline
        result['accepted'] = bool(guardian_ok and not result['outer_timed_out'] and not pending
                                  and result['cleanup_complete'] and result['g4_evidence_accepted']
                                  and result['crypto_evidence_accepted'] and result['source_binding_valid']
                                  and result['artifact_integrity_valid'] and not result['failures'])
        if not result['accepted']:
            result['primary_failure'] = result['primary_failure'] or ('outer_timeout' if result['outer_timed_out'] else 'evidence_rejected_or_unknown')
    except BaseException as exc:
        if result is None:
            raise  # Eligibility rejection: no launch reservation/attempt.
        result['accepted'] = False
        result['primary_failure'] = result['primary_failure'] or 'collector_exception'
        result['collector_errors'].append(type(exc).__name__)
        result['failures'].append(dict(phase='collector', reason=stage))
    finally:
        for n, handler in old.items():
            signal.signal(n, handler)
        try:
            if result is not None:
                # Main-thread commit boundary: both owned handlers have retired.
                # pending cannot gain another entry from our signal handlers now.
                result['interrupts'] = list(pending)
                result['end_ns'] = time.monotonic_ns()
                result['outer_timed_out'] |= result['end_ns'] >= result['deadline_ns']
                if result['interrupts']:
                    result['primary_failure'] = result['primary_failure'] or 'collector_interrupted'
                    result['failures'].append(dict(phase='collector', reason='collector_interrupted'))
                result['accepted'] = (result['accepted'] and not result['interrupts']
                                      and not result['outer_timed_out'])
                b.write_private(out/'outer-result.json', b.encoded(result))
        finally:
            os.close(lockfd)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'run'))
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    try:
        if args.action == 'prepare':
            prepare(args.config)
            return 0
        result = collect(args.config)
        # Publication I/O can fail or stall; a late return must still fail closed.
        return 0 if (result['accepted'] and result.get('interrupts') == []
                     and time.monotonic_ns() < result['deadline_ns']) else 1
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('D2 collector rejected: ' + type(exc).__name__, file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
