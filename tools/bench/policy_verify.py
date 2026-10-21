"""Read-only G2 evidence verification. Never imports or executes the runner."""
import argparse
import json
from pathlib import Path
import sys

from . import policy_binding as b

BASE = ('supervised/supervisor.json', 'supervised/resources.jsonl',
        'supervised/runner.stdout', 'supervised/runner.stderr')
INNER = ('started.json', 'owner.json', 'result.json', 'stdout', 'stderr')


def identity(value):
    return (isinstance(value, dict) and all(type(value.get(k)) is int and value[k] > 1
            for k in ('pid', 'pgid', 'sid')) and value['pid'] == value['pgid'] == value['sid']
            and type(value.get('start_ticks')) is int and value['start_ticks'] >= 0)


def observed_outcome(out, manifest, policy):
    """Reconstruct status and hashes from evidence; not a process-start oracle."""
    out = Path(out)
    artifacts, missing = {}, []
    for name in (*BASE, *('supervised/inner/' + n for n in INNER)):
        path = out / name
        if not path.exists():
            missing.append(name)
            continue
        b.require(not any(p.is_symlink() for p in path.parents if p != out.parent), 'evidence parent symlink')
        data = b.read_regular(path)
        artifacts[name] = dict(bytes=len(data), sha256=b.sha(data))
    value = dict(schema=1, run_id=manifest['run_id'], state='evidence_incomplete',
                 manifest_sha256=b.sha(b.read_regular(out / 'manifest.json')),
                 artifacts=artifacts, missing_evidence=missing, runner_owner=None, target_owner=None,
                 runner_spawned=False, target_spawned=False, run_succeeded=False)
    if BASE[0] not in artifacts:
        return value
    s = b.decode(b.read_regular(out / BASE[0]))
    b.require(isinstance(s, dict), 'malformed supervisor record')
    supervisor_cleanup = s.get('cleanup')
    b.require(isinstance(supervisor_cleanup, dict), 'malformed supervisor cleanup')
    b.require(type(supervisor_cleanup.get('success')) is bool
              and 'direct_reaped' in supervisor_cleanup
              and type(supervisor_cleanup['direct_reaped']) in (bool, type(None))
              and isinstance(supervisor_cleanup.get('errors'), list)
              and 'residual_live' in supervisor_cleanup
              and (isinstance(supervisor_cleanup['residual_live'], list)
                   or (supervisor_cleanup['success'] is False and supervisor_cleanup['residual_live'] is None)),
              'malformed supervisor cleanup fields')
    b.require(s.get('runner_sha256') == b.APPROVED['run_check.py']
              and s.get('supervisor_sha256') == b.APPROVED['supervise.py'], 'supervisor source disagreement')
    b.require(s.get('target_argv') == manifest['target_argv']
              and s.get('runner_argv') == manifest['runner_argv'] and s.get('role') == 'probe', 'actual argv mismatch')
    for key, source in [('barrier_seconds', 'barrier_seconds'), ('outer_seconds', 'outer_seconds'), ('inner_seconds', 'wall_seconds')]:
        b.require(type(s.get(key)) in (int, float) and s[key] == policy[source], 'supervisor budget mismatch')
    safe_keys = {'GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS', 'DZKR_MIMCCHAIN_START_BARRIER',
                 'DZKR_MIMCCHAIN_BARRIER_TIMEOUT', 'DZKR_MIMCCHAIN_BARRIER_PROBE'}
    b.require(s.get('safe_environment') == {k: manifest['environment'][k] for k in safe_keys}, 'actual environment mismatch')
    value['runner_owner'], value['target_owner'] = s.get('runner_owner'), s.get('target_owner')
    value['runner_spawned'] = identity(value['runner_owner'])
    value['target_spawned'] = identity(value['target_owner'])
    if not value['runner_spawned']:
        b.require(value['runner_owner'] is None and value['target_owner'] is None
                  and s.get('inner_result') is None and not s.get('observation_complete')
                  and not any(n.startswith('supervised/inner/') for n in artifacts),
                  'spawn failure contradicts execution evidence')
        if s.get('reason') and s.get('errors'):
            value['state'] = 'spawn_failed'
        return value
    if missing:
        return value
    runner_exit = supervisor_cleanup.get('runner_exit_code')
    b.require(type(runner_exit) is int, 'missing or malformed Runner exit status')
    b.require(type(supervisor_cleanup['direct_reaped']) is bool, 'malformed Runner reap status')
    inner = out / 'supervised/inner'
    started, result, owner = (b.decode(b.read_regular(inner / n)) for n in ('started.json', 'result.json', 'owner.json'))
    b.require(all(isinstance(r, dict) for r in (started, result, owner)), 'malformed runner records')
    target_cleanup = result.get('cleanup')
    b.require(isinstance(target_cleanup, dict), 'malformed target cleanup')
    b.require(type(target_cleanup.get('success')) is bool
              and type(target_cleanup.get('direct_reaped')) is bool
              and isinstance(target_cleanup.get('errors'), list)
              and 'direct_exit_code' in target_cleanup
              and (type(target_cleanup['direct_exit_code']) is int
                   or (target_cleanup['success'] is False and target_cleanup['direct_exit_code'] is None)),
              'malformed target cleanup fields')
    if target_cleanup['success'] or 'final' in target_cleanup:
        final = target_cleanup.get('final')
        b.require(isinstance(final, dict) and isinstance(final.get('live'), list)
                  and isinstance(final.get('errors'), list), 'malformed final cleanup observation')
    primary = result.get('primary')
    b.require(isinstance(primary, dict) and isinstance(primary.get('outcome'), str), 'malformed target outcome')
    b.require(identity(owner) and owner == result.get('owner') == s.get('target_owner'), 'target ownership mismatch')
    b.require(s.get('inner_result') == result, 'embedded runner evidence disagreement')
    b.require(s.get('inner_primary') == result.get('primary')
              and s.get('inner_cleanup') == result.get('cleanup'), 'supervisor runner summary disagreement')
    # Basic trace integrity only: G2 does not recompute RSS or membership peaks.
    rows = [b.decode(line) for line in b.read_regular(out / 'supervised/resources.jsonl').splitlines() if line.strip()]
    b.require(rows and all(isinstance(row, dict) for row in rows), 'missing or malformed resource trace')
    pre = s.get('pre_release_live_sample')
    b.require(isinstance(pre, dict) and pre in rows and pre.get('valid') is True
              and pre.get('live') is True
              and isinstance(pre.get('identity'), dict)
              and all(pre['identity'].get(k) == owner[k] for k in ('pid', 'pgid', 'sid', 'start_ticks')),
              'readiness trace disagreement')
    expected = {k: v for k, v in b.FIXED.items() if k != 'core_bytes'}
    expected['wall_seconds'] = float(policy['wall_seconds'])
    for record in (started, result):
        b.require(record.get('argv') == manifest['target_argv'] and record.get('cwd') == manifest['cwd'], 'runner target/cwd disagreement')
        b.require(b.encoded(record.get('limits')) == b.encoded(expected), 'actual runner limits mismatch')
        b.require(record.get('environment') == {k: manifest['environment'][k] for k in ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS')}
                  and record.get('opt_in_flags') == {}, 'runner environment mismatch')
    raw = result.get('raw')
    b.require(isinstance(raw, dict), 'malformed raw output evidence')
    for name in ('stdout', 'stderr'):
        b.require(raw.get(name) == artifacts['supervised/inner/' + name], 'runner output hash mismatch')
    if (not supervisor_cleanup['success'] or supervisor_cleanup['residual_live'] != []
            or not target_cleanup['success']):
        value['state'] = 'cleanup_failed'
    elif (result.get('evidence_complete') is not True or result.get('evidence_errors')
          or s.get('errors') or result.get('execution_errors')):
        value['state'] = 'evidence_incomplete'
    elif (runner_exit != 0 or s.get('success') is not True or result.get('overall_success') is not True
          or primary['outcome'] != 'command_success'
          or result.get('runner_signals') or result.get('stop_escalation')
          or target_cleanup['direct_exit_code'] != 0):
        value['state'] = 'runner_failed_or_stopped'
    else:
        b.require(s.get('reason') is None and s.get('observation_complete') is True
                  and supervisor_cleanup['direct_reaped'] is True
                  and supervisor_cleanup['errors'] == []
                  and target_cleanup['direct_reaped'] is True
                  and target_cleanup['errors'] == []
                  and target_cleanup['final']['live'] == []
                  and target_cleanup['final']['errors'] == [], 'contradictory successful cleanup/observation')
        value.update(state='target_completed', run_succeeded=True)
    return value


def verify(out, *, prepared=False):
    m, policy = b.check_binding(out)
    out = Path(out)
    if prepared:
        b.require(not (out / 'execution.json').exists() and not (out / 'outcome.json').exists(), 'run is not merely prepared')
        return dict(binding_valid=True, run_id=m['run_id'], state='prepared', run_succeeded=False,
                    policy_sha256=m['policy']['sha256'])
    request = b.decode(b.read_regular(out / 'execution.json'))
    expected = dict(schema=1, run_id=m['run_id'], state='launch_requested', manifest_sha256=b.sha(b.read_regular(out / 'manifest.json')))
    b.require(b.encoded(request) == b.encoded(expected), 'execution identity mismatch')
    outcome = b.decode(b.read_regular(out / 'outcome.json'))
    derived = observed_outcome(out, m, policy)
    b.require(b.encoded(outcome) == b.encoded(derived), 'outcome/evidence disagreement')
    return dict(binding_valid=True, run_id=m['run_id'], state=derived['state'], run_succeeded=derived['run_succeeded'],
                runner_spawned=derived['runner_spawned'], target_spawned=derived['target_spawned'],
                policy_sha256=m['policy']['sha256'])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('out')
    p.add_argument('--prepared', action='store_true', help='verify intent only, never claim an executed run')
    args = p.parse_args()
    try:
        result = verify(args.out, prepared=args.prepared)
        print(json.dumps(result))
        return 0 if args.prepared or result['run_succeeded'] else 1
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('invalid policy evidence: ' + (str(exc) if isinstance(exc, b.PolicyError) else type(exc).__name__), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
