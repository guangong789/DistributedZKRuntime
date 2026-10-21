"""Bounded synthetic G3 case, invoked only by an independent outer guardian.

The file barrier supplies explicit phase evidence to the adapter API; it is not
a replacement for the production Go barrier or G2's existing supervisor CLI.
"""
import json
import os
from pathlib import Path
import select
import sys
import time

from tools.bench import group_run, processes


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def main():
    require(os.environ.get('DZKR_G3_GUARDIAN_PID') == str(os.getppid()), 'independent guardian required')
    mode, work = sys.argv[1], Path(sys.argv[2])
    require(mode in ('single', 'child'), 'known synthetic mode')
    control = work / 'control'
    control.mkdir(mode=0o700)
    out = work / 'run'
    phase = None

    def wait_json(path):
        end = time.monotonic() + .6
        while time.monotonic() < end:
            try:
                return json.loads(path.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                select.select([], [], [], .005)
        raise RuntimeError('synthetic barrier deadline')

    def phase_provider(owner):
        nonlocal phase
        if phase is None:
            ready = wait_json(control / 'ready.json')
            require(ready['pid'] == owner['pid'] and processes.same(owner, processes.identity(owner['pid'])), 'ready identity')
            if mode == 'child':
                child = processes.identity(ready['child'])
                require(child and child['sid'] == owner['sid'] and child['pgid'] == owner['pgid'], 'child membership')
            phase = dict(name='readiness', event=dict(kind='ready_verified', owner=owner,
                         monotonic_ns=time.monotonic_ns()))
        elif phase['name'] == 'readiness':
            # The previous monitor has returned only after persisting readiness.
            (control / 'release').write_bytes(b'release\n')
            ack = wait_json(control / 'ack.json')
            require(ack['pid'] == owner['pid'] and processes.same(owner, processes.identity(owner['pid'])), 'ack identity')
            phase = dict(name='workload', event=dict(kind='release_acknowledged', owner=owner,
                         monotonic_ns=ack['monotonic_ns']))
        return phase

    argv = [sys.executable, '-B', str(Path(__file__).with_name('fixtures') / 'group_peer.py'),
            mode, str(control), str(out / 'group-rss.jsonl')]
    result = group_run.run(argv, out, wall_seconds=2, phase_provider=phase_provider)
    require(result['success'], 'G3 synthetic execution/audit failed')
    owner = json.loads((out / 'inner/owner.json').read_text())
    current, members = processes.identity(owner['pid']), processes.members(owner)
    cleanup = dict(owner=owner, current=current, residual_live=members,
                   runner=result['cleanup'])
    (work / 'independent-cleanup.json').write_text(json.dumps(cleanup, indent=2) + '\n')
    require(not members and (current is None or current['state'] in ('Z', 'X')), 'live synthetic residue')
    require(result['cleanup']['success'] and result['cleanup']['direct_reaped'], 'Runner cleanup')
    rows = [json.loads(line) for line in (out / 'group-rss.jsonl').read_text().splitlines()]
    require(any(r['sample_status'] == 'COMPLETE' and r['phase']['name'] == 'workload'
                and len(r['members']) >= (2 if mode == 'child' else 1) for r in rows), 'missing known live group')
    require(result['audit']['phases']['readiness']['complete'] >= 1 and result['audit']['phases']['workload']['complete'] >= 1, 'phase separation')
    require(rows[-1]['sample_status'] in ('NO_LIVE_MEMBERS', 'ZOMBIE_ONLY'), 'post-cleanup absence')
    print(json.dumps(dict(case=mode, passed=True, samples=len(rows), crypto_operations=0)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
