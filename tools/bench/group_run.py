"""Opt-in G3 adapter using the approved Runner's existing monitor seam.

The G2 supervisor CLI is unchanged and does not enable this stream. Without an
explicit embedding barrier event, monitor samples are unknown, never workload.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from . import group_audit, group_rss, run_check
from .group_schema import CONTRACT, KEYS, EvidenceError

APPROVED_RUNNER = 'b31448c0114902b9f4888077e26f6153453c7f413bf73ad58244a31c3cd6b9ce'


def write(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')


def run(argv, out, *, wall_seconds=2, phase_provider=None):
    """phase_provider(owner) may supply explicit identity-bound barrier events.

    No phase provider is inferred from stdout or elapsed time. The approved
    Runner owns launch, limits, signal handling, WNOWAIT and cleanup unchanged.
    """
    out = Path(out).resolve()
    if out.is_relative_to(Path(__file__).resolve().parents[2]):
        raise EvidenceError('external output directory required')
    if hashlib.sha256(Path(run_check.__file__).read_bytes()).hexdigest() != APPROVED_RUNNER:
        raise EvidenceError('approved Runner identity changed')
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    sources = {}
    (out / 'sources').mkdir(mode=0o700)
    for name in ('run_check.py', 'group_schema.py', 'group_rss.py', 'group_audit.py', 'group_run.py'):
        data = Path(__file__).with_name(name).read_bytes()
        (out / 'sources' / name).write_bytes(data)
        sources[name] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    write(out / 'manifest.json', dict(contract=CONTRACT, run_id=run_id,
          sources=sources, policy_snapshot_sha256=None, argv=argv,
          sampling='approved Runner monitor events; nominal 50ms plus observation/loop overhead',
          phase='unknown unless explicit identity-bound barrier event supplied',
          legacy_g2_compatibility='separate opt-in stream; G2 launch unchanged'))
    recorder = group_rss.Recorder(out / 'group-rss.jsonl', run_id)
    owner = result = None

    def monitor(pgid):
        nonlocal owner
        if owner is None:
            owner = json.loads((out / 'inner/owner.json').read_text())
        if owner['pid'] != pgid:
            raise EvidenceError('monitor ownership disagreement')
        phase = phase_provider(owner) if phase_provider else None
        row = group_rss.observe(owner, run_id, recorder.count, phase)
        recorder.append(row)
        if row['sample_status'] not in ('COMPLETE', 'ZOMBIE_ONLY', 'NO_LIVE_MEMBERS'):
            raise EvidenceError('group sample ' + row['sample_status'])
        # Zero here means verified no-live, never unreadable/missing RSS.
        state = dict(live=[m['after'] for m in row['members'] if m['status'] == 'LIVE'],
                     zombies=[m['after'] for m in row['members'] if m['status'] == 'ZOMBIE'], errors=[],
                     rss_kib=(row['group_rss_bytes'] // 1024 if row['sample_status'] == 'COMPLETE' else 0))
        return state, row['mem_available_bytes'] // 1024

    try:
        result = run_check.run_command(argv, out / 'inner', wall_seconds=wall_seconds,
                                       term_grace=.15, kill_grace=.5, monitor=monitor)
        if owner is not None and not recorder.error:
            phase = dict(name='cleanup', event=dict(kind='cleanup_completed',
                         monotonic_ns=time.monotonic_ns(), owner={k: owner[k] for k in KEYS}))
            row = group_rss.observe(owner, run_id, recorder.count, phase, site='cleanup_check')
            recorder.append(row)
    finally:
        summary = recorder.close(result['sampled_group_peak_rss_kib'] * 1024 if result else None)
        write(out / 'group-summary.json', summary)
    error, audited = None, None
    try:
        audited = group_audit.audit(out / 'group-rss.jsonl', out / 'group-summary.json')
    except (OSError, ValueError, TypeError, KeyError) as exc:
        error = type(exc).__name__ + ':' + str(exc)
    complete_monitor = (summary['samples'] > 0 and summary['status_counts']['PARTIAL'] == 0
                        and summary['status_counts']['INVALID'] == 0 and summary['runner_monitor_peak_bytes'] is not None)
    success = bool(result['overall_success'] and audited and complete_monitor
                   and summary['storage_complete'] and result['cleanup']['success'])
    report = dict(success=success, run_id=run_id, audit=audited, audit_error=error,
                  cleanup=result['cleanup'], legacy_runner_success=result['overall_success'],
                  workload_coverage=summary['workload_coverage'], actual_lifetime_peak_bytes=None)
    write(out / 'g3-result.json', report)
    return report


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    parser.add_argument('--wall-seconds', type=float, default=2)
    parser.add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
    result = run(argv, args.out, wall_seconds=args.wall_seconds)
    print(json.dumps(dict(success=result['success'], run_id=result['run_id'],
                          workload_coverage=result['workload_coverage'])))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
