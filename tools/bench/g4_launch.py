"""One opt-in synthetic attempt; reuse accepted Supervisor and harness containment."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

from . import g4_binding as binding, g4_supervise, policy_binding as b, processes as ps
from .tests import harness


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')


def available():
    return next(int(x.split()[1]) * 1024 for x in Path('/proc/meminfo').read_text().splitlines()
                if x.startswith('MemAvailable:'))


def observe_launch(pid):
    """Read launch facts only, never another RSS stream or a plaintext token."""
    before = ps.identity(pid)
    if before is None or before['state'] in ('Z', 'X'):
        return None
    try:
        image = ps.executable(pid)
        argv = Path('/proc/%d/cmdline' % pid).read_bytes().rstrip(b'\0').split(b'\0')
        environment = dict(x.decode().split('=', 1) for x in
                           Path('/proc/%d/environ' % pid).read_bytes().split(b'\0') if x)
        token = environment.pop('DZKR_MIMCCHAIN_BARRIER_TOKEN', None)
        after = ps.identity(pid)
        if not ps.same(before, after) or after['state'] in ('Z', 'X') or ps.executable(pid) != image:
            return None
        return dict(identity=after, executable=image, argv=[x.decode() for x in argv],
                    environment=environment, barrier_token_sha256=b.sha(token.encode()) if token else None)
    except (FileNotFoundError, ProcessLookupError):
        return None


def launch(config):
    m = binding.check(config, live=True)
    out = Path(m['paths']['out'])
    b.require(sys.platform == 'linux' and hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'),
              'Linux pidfd containment required')
    minimum = m['crypto']['minimum_available_bytes'] if binding.is_crypto(m) else 4 * 1024**3
    guardian_seconds = m['crypto']['guardian_seconds'] if binding.is_crypto(m) else 15
    b.require(available() >= minimum and os.getloadavg()[0] < os.cpu_count(), 'resource preflight failed')
    b.require(not Path(m['paths']['channel']).parent.exists(), 'private IPC path already exists')
    b.require(not harness.children(os.getpid()), 'guardian must start without children')
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER')
    # Exclusive marker consumes the one-attempt allowance before spawning.
    write(out / 'attempt.json', dict(run_id=m['run_id'], config_sha256=b.sha(b.read_regular(config)),
                                   guardian=ps.identity(os.getpid()), monotonic_ns=time.monotonic_ns()))
    argv = [m['python']['path'], '-B', '-m', 'tools.bench.g4_launch', '--supervisor', str(config)]
    rec = dict(run_id=m['run_id'], argv=argv, stop=None, signals=[], residual_children=None,
               observations={}, peak_descendants=0, minimum_memavailable_bytes=available(),
               outer_seconds=guardian_seconds, maximum_descendants=8, start_ns=time.monotonic_ns())
    pending = []
    old = {n: signal.signal(n, lambda number, frame: pending.append(number))
           for n in (signal.SIGTERM, signal.SIGINT)}
    proc = None
    try:
        with (out / 'supervisor.stdout').open('xb') as stdout, (out / 'supervisor.stderr').open('xb') as stderr:
            proc = subprocess.Popen(argv, cwd=m['cwd'], env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'),
                                    stdout=stdout, stderr=stderr, start_new_session=True)
            rec['supervisor_owner'] = ps.identity(proc.pid)
            while os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                harness.reap_adopted(proc.pid)
                descendants = harness.descendants(os.getpid())
                rec['peak_descendants'] = max(rec['peak_descendants'], len(descendants))
                rec['minimum_memavailable_bytes'] = min(rec['minimum_memavailable_bytes'], available())
                for pid in descendants - {proc.pid}:
                    info = ps.identity(pid)
                    if info is None:
                        continue
                    role = 'bootstrap' if info['ppid'] == proc.pid else 'target'
                    if role not in rec['observations']:
                        observation = observe_launch(pid)
                        if observation is not None:
                            rec['observations'][role] = observation
                if pending or time.monotonic_ns() - rec['start_ns'] > guardian_seconds * 1_000_000_000:
                    rec['stop'] = 'interrupt_or_outer_timeout'
                    break
                if len(descendants) > 8 or rec['minimum_memavailable_bytes'] < 2 * 1024**3:
                    rec['stop'] = 'process_count_or_headroom_stop'
                    break
                select.select([], [], [], .005)
            if rec['stop'] is None:
                harness.reap_adopted(proc.pid)
                if any(pid != proc.pid for pid in harness.children(os.getpid())):
                    rec['stop'] = 'unexpected_surviving_child'
                else:
                    rec['exit_code'] = proc.wait(timeout=1)
                    harness.reap_adopted()
                    rec['residual_children'] = harness.children(os.getpid())
            if rec['stop'] is not None:
                rec['residual_children'] = harness.cleanup(proc, rec['signals'])
    except BaseException as exc:
        rec.update(stop='guardian_error', error=dict(type=type(exc).__name__, message=str(exc)))
        if proc is not None and proc.returncode is None:
            rec['residual_children'] = harness.cleanup(proc, rec['signals'])
    finally:
        for number, handler in old.items():
            signal.signal(number, handler)
        rec['end_ns'] = time.monotonic_ns()
        write(out / 'guardian.json', rec)
    return rec


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config')
    parser.add_argument('--supervisor')
    args = parser.parse_args()
    b.require(bool(args.config) != bool(args.supervisor), 'choose one launch mode')
    if args.supervisor:
        m = binding.check(args.supervisor, live=True)
        result = g4_supervise.supervise(m['target_argv'], m['paths']['supervised'],
                                      entrypoint=m['entrypoint'], config=args.supervisor)
        return 0 if result['success'] else 1
    return 0 if launch(args.config).get('exit_code') == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
