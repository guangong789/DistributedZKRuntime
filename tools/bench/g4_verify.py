"""Read-only same-run G4 validator; no process launch or signaling."""
import argparse
import json
import os
from pathlib import Path

from . import g4_binding as binding, g4_events as events, group_audit, policy_binding as b

REQUIRED = ('g4.json', 'policy.snapshot.json', 'attempt.json', 'guardian.json',
            'supervisor.stdout', 'supervisor.stderr', 'supervisor-events.jsonl',
            'supervised/bootstrap-events.jsonl', 'supervised/bootstrap.json',
            'supervised/supervisor.json', 'supervised/resources.jsonl',
            'supervised/group-rss.jsonl', 'supervised/group-summary.json',
            'supervised/inner/started.json', 'supervised/inner/owner.json',
            'supervised/inner/result.json', 'supervised/inner/stdout', 'supervised/inner/stderr')


def encoded_equal(a, c):
    return b.encoded(a) == b.encoded(c)


def artifact_names(crypto=False):
    names = (*REQUIRED, *('sources/' + name for name in binding.SOURCES))
    if crypto:
        from .g4_crypto import EXTRA_ARTIFACTS
        return (*names, *EXTRA_ARTIFACTS)
    return names


def seal(out):
    """Index exact required artifacts; this index is consistency, not attestation."""
    out = Path(out)
    records = {}
    manifest = b.decode(b.read_regular(out / 'g4.json'))
    for name in artifact_names(binding.is_crypto(manifest)):
        raw = b.read_regular(out / name, 16 * 1024 * 1024)
        records[name] = dict(bytes=len(raw), sha256=b.sha(raw))
    b.write_private(out / 'g4-artifacts.json', b.encoded(records))


def resource_decision(monitor, core, limits):
    """Reconcile G3-validated inputs with run_check's KiB decisions, not its flags."""
    require = b.require
    require(monitor, 'resource policy: missing common monitor samples')
    peak, minimum, trigger = 0, None, None
    for n, row in enumerate(monitor):
        require(row['phase']['name'] in ('readiness', 'workload')
                and row['sample_status'] in ('COMPLETE', 'ZOMBIE_ONLY', 'NO_LIVE_MEMBERS'),
                'resource policy: ineligible monitor sample')
        require(type(row.get('mem_available_bytes')) is int, 'resource policy: missing MemAvailable')
        # Bootstrap returns zero for non-live observations, not a complete RSS sample.
        rss_kib = row['group_rss_bytes'] // 1024 if row['sample_status'] == 'COMPLETE' else 0
        available_kib = row['mem_available_bytes'] // 1024
        peak = max(peak, rss_kib)
        minimum = available_kib if minimum is None else min(minimum, available_kib)
        reason = ('group RSS' if rss_kib > limits['rss_stop_kib'] else
                  'system MemAvailable' if available_kib < limits['available_stop_kib'] else None)
        if reason is not None and trigger is None:
            trigger = (n, reason)
    require(type(core.get('sampled_group_peak_rss_kib')) is int
            and core['sampled_group_peak_rss_kib'] == peak, 'resource policy: published peak disagreement')
    require(type(core.get('minimum_memavailable_kib')) is int
            and core['minimum_memavailable_kib'] == minimum, 'resource policy: same-sample MemAvailable')
    escalation = peak > limits['stop_escalation_kib']
    require(type(core.get('stop_escalation')) is bool and core['stop_escalation'] is escalation,
            'resource policy: stop_escalation disagreement')
    primary = core['primary']
    outcome = primary['outcome']
    if trigger is not None:
        require(outcome != 'command_success', 'resource policy: trigger contradicts command_success')
        require(trigger[0] == len(monitor) - 1, 'resource policy: monitor continued after stop trigger')
        # Parsing/phase watchdog checks precede the resource branch after monitor returns.
        preempted = outcome == 'runner_error' or (outcome == 'timeout' and primary.get('reason') in
                    {'phase watchdog: ' + name for name in limits['phase_seconds']})
        require(outcome == 'resource_stop' or preempted, 'resource policy: trigger/outcome disagreement')
    if outcome == 'resource_stop':
        require(trigger is not None, 'resource policy: stop without common trigger')
        require(primary.get('reason') == trigger[1], 'resource policy: stop reason disagreement')
    return dict(monitor_samples=len(monitor), sampled_peak_kib=peak, minimum_memavailable_kib=minimum,
                observed_stop_required=trigger is not None,
                first_trigger_sequence=monitor[trigger[0]]['sequence'] if trigger else None,
                stop_reason=trigger[1] if trigger else None, stop_escalation=escalation)


def audit(out):
    report = dict(execution_attempted=None, execution_succeeded=None, evidence_valid=False,
                  workload_coverage='unknown', final_accepted=False, errors=[])
    try:
        _audit(Path(out), report)
    except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError, OverflowError) as exc:
        report['errors'].append(dict(type=type(exc).__name__, message=str(exc)))
    return report


def _audit(out, report):
    require = b.require
    index = b.decode(b.read_regular(out / 'g4-artifacts.json'))
    manifest = b.decode(b.read_regular(out / 'g4.json'))
    crypto = binding.is_crypto(manifest)
    require(isinstance(index, dict) and set(index) == set(artifact_names(crypto)), 'artifact set')
    raw = {}
    for name, identity in index.items():
        data = b.read_regular(out / name, 16 * 1024 * 1024)
        require(encoded_equal(identity, dict(bytes=len(data), sha256=b.sha(data))), 'artifact identity: ' + name)
        raw[name] = data
    m = binding.check(out / 'g4.json', live=True)
    require(m['paths']['out'] == str(out.resolve()), 'bundle path identity')
    report['run_id'] = m['run_id']
    if crypto:
        report['crypto'] = dict(attempted=None, setup_succeeded=None, prove_calls_succeeded=None,
                               verify_calls_succeeded=None, wrong_digest_rejected=None,
                               timing_evidence_present=False, evidence_valid=False)
    load = lambda name: b.decode(raw[name])
    s, g = load('supervised/supervisor.json'), load('guardian.json')
    core, started = load('supervised/inner/result.json'), load('supervised/inner/started.json')
    boot, attempt = load('supervised/bootstrap.json'), load('attempt.json')
    for value in (s, g, core, started, boot, attempt):
        require(isinstance(value, dict), 'artifact mapping')
    target = load('supervised/inner/owner.json')
    target_id, runner_id, supervisor_id = (events.identity(x) for x in
                                         (target, s['runner_owner'], g['supervisor_owner']))
    require(len({x['pid'] for x in (target_id, runner_id, supervisor_id)}) == 3, 'distinct process owners')
    require(target['ppid'] == runner_id['pid'] and s['runner_owner']['ppid'] == supervisor_id['pid'],
            'owned process parent chain')
    require(encoded_equal(events.identity(s['target_owner']), target_id)
            and encoded_equal(events.identity(core['owner']), target_id), 'target generation')
    report['execution_attempted'] = True
    require(g['run_id'] == attempt['run_id'] == boot['run_id'] == s['run_id'] == m['run_id'], 'run ID')
    require(g['argv'] == [m['python']['path'], '-B', '-m', 'tools.bench.g4_launch', '--supervisor',
                           m['paths']['config']], 'actual Supervisor argv')
    config_sha = b.sha(raw['g4.json'])
    require(attempt['config_sha256'] == s['g4_config_sha256'] == config_sha, 'config identity')
    require(s['g4_contract'] == boot['contract'] == binding.CONTRACT, 'contract version')
    require(encoded_equal(boot['supervisor_owner'], supervisor_id)
            and encoded_equal(s['g4_channel_peer'], runner_id), 'coordination peer identity')
    require(g['supervisor_owner']['ppid'] == attempt['guardian']['pid'], 'guardian parent')
    require(s['peer']['pid'] == target_id['pid'] and s['peer']['uid'] == m['directory']['uid'], 'Go peer identity')
    require(s['runner_sha256'] == m['sources']['run_check.py']['sha256']
            and s['supervisor_sha256'] == m['sources']['g4_supervise.py']['sha256'], 'executed source identity')
    for container in (started, core):
        require(container['argv'] == m['target_argv'] and container['cwd'] == m['cwd'], 'actual target argv/cwd')
        require(container['environment'] == {k: m['environment'][k] for k in
                                            ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS')}, 'Runner environment')
        limits = container['limits']
        require(limits['wall_seconds'] == m['effective_limits']['wall_seconds'], 'actual wall budget')
        for key in b.FIXED:
            if key != 'core_bytes':
                require(encoded_equal(limits[key], m['effective_limits'][key]), 'actual limit: ' + key)
    require(s['runner_argv'] == m['runner_argv'] and s['target_argv'] == m['target_argv'], 'actual Bootstrap argv')
    safe_keys = ('GOTOOLCHAIN', 'GOPROXY', 'GOSUMDB', 'GOMAXPROCS', 'DZKR_MIMCCHAIN_START_BARRIER',
                 'DZKR_MIMCCHAIN_BARRIER_TIMEOUT', 'DZKR_MIMCCHAIN_BARRIER_PROBE')
    if crypto:
        safe_keys = (*safe_keys[:-1], 'DZKR_MIMCCHAIN_CPU_PILOT')
    require(s['safe_environment'] == {k: m['environment'][k] for k in safe_keys}, 'Supervisor environment')
    for key, name in (('inner_seconds', 'wall_seconds'), ('barrier_seconds', 'barrier_seconds'),
                      ('outer_seconds', 'outer_seconds')):
        require(s[key] == m['effective_limits'][name], 'Supervisor budget')
    for role, identity, argv in (('bootstrap', runner_id, m['runner_argv']), ('target', target_id, m['target_argv'])):
        obs = g['observations'][role]
        require(encoded_equal(events.identity(obs['identity']), identity) and obs['argv'] == argv, 'observed argv/generation')
        environment = dict(obs['environment'])
        require(environment.pop('DZKR_MIMCCHAIN_BARRIER_SOCKET') == s['ipc']['socket_path'], 'observed barrier socket')
        require(environment == m['environment'], 'observed environment')
        require(obs['barrier_token_sha256'] == s['ipc']['token_sha256'], 'observed token hash')
        expected = m['python'] if role == 'bootstrap' else m['target_executable']
        require(all(obs['executable'][k] == expected[k] for k in ('path', 'device', 'inode', 'sha256')), 'observed executable')
    executable = b.read_regular(m['target_executable']['path'], 64 * 1024 * 1024)
    require(len(executable) == m['target_executable']['bytes']
            and b.sha(executable) == m['target_executable']['sha256'], 'target executable SHA')
    expected_image = {k: m['target_executable'][k] for k in ('path', 'device', 'inode', 'sha256')}
    require(s['expected_executable'] == s['target_executable'] == expected_image, 'Supervisor executable evidence')
    a = [b.decode(line) for line in raw['supervisor-events.jsonl'].splitlines()]
    c = [b.decode(line) for line in raw['supervised/bootstrap-events.jsonl'].splitlines()]
    require(encoded_equal(a, c) and len(a) == 5, 'paired event transcripts')
    state = events.State(m['run_id'], clock=lambda: g['end_ns'])
    for message in a:
        require(g['start_ns'] <= message['monotonic_ns'] <= g['end_ns'], 'guardian event interval')
        state.accept(message)
        require(message['owner'] == target_id, 'Barrier owner')
    require(state.sequence == 5 and not state.failed, 'event completion')
    supervisor_events = {e['event']: e for e in s['events']}
    require(a[0]['monotonic_ns'] == supervisor_events['target_identified']['monotonic_ns']
            and a[2]['monotonic_ns'] == s['release_begin_ns']
            and a[3]['monotonic_ns'] == s['ack_ns'], 'verified ready/release/ack times')
    for sequence, phase in ((1, 'readiness'), (4, 'workload')):
        events.verify_receipt(m, a[sequence], phase)
    rss = group_audit.audit(out / 'supervised/group-rss.jsonl', out / 'supervised/group-summary.json')
    require(not rss['has_partial'] and rss['status_counts']['INVALID'] == 0, 'partial RSS evidence')
    require(rss['phases']['readiness']['complete'] > 0 and rss['phases']['workload']['complete'] > 0,
            'required phase coverage')
    rows = [b.decode(line) for line in raw['supervised/group-rss.jsonl'].splitlines()]
    require(all(row['run_id'] == m['run_id'] and row['owner'] == target_id for row in rows), 'RSS run/owner')
    require(rows[0]['phase']['event']['monotonic_ns'] == a[0]['monotonic_ns']
            and rows[1]['phase']['event']['monotonic_ns'] == a[3]['monotonic_ns'], 'sample phase source')
    require(all(row['phase']['name'] == 'cleanup' or row['site'] == 'runner_monitor' for row in rows), 'common monitor site')
    require(rows[-1]['site'] == 'cleanup_check' and rows[-1]['sample_status'] == 'NO_LIVE_MEMBERS', 'terminal absence sample')
    monitor = [row for row in rows if row['site'] == 'runner_monitor']
    resource = resource_decision(monitor, core, m['effective_limits'])
    require(encoded_equal(boot['audit'], rss), 'independent G3 audit agreement')
    require(s['inner_result'] == core and s['inner_primary'] == core['primary']
            and s['inner_cleanup'] == core['cleanup'], 'inner result copies')
    for name in ('stdout', 'stderr'):
        data = raw['supervised/inner/' + name]
        require(core['raw'][name] == dict(bytes=len(data), sha256=b.sha(data)), 'target output identity')
    tc, sc = core['cleanup'], s['cleanup']
    for container, name in ((core, 'evidence_errors'), (core, 'runner_signals'), (core, 'execution_errors'),
                            (tc, 'errors'), (sc, 'errors'), (s, 'errors')):
        require(isinstance(container.get(name, []), list), 'malformed error/signal list')
    require(tc['direct_reaped'] is True and sc['direct_reaped'] is True
            and tc['success'] is True and sc['success'] is True
            and tc['final']['live'] == [] and tc['final']['errors'] == []
            and sc['residual_live'] == [] and not tc['errors'] and not sc['errors'], 'owned cleanup incomplete')
    require(g['stop'] is None and g['signals'] == [] and g['residual_children'] == [], 'guardian cleanup/stop')
    require(g['peak_descendants'] <= 8 and g['minimum_memavailable_bytes'] >= 2 * 1024**3, 'guardian safety bounds')
    require(type(g['peak_descendants']) is int and type(g['minimum_memavailable_bytes']) is int, 'guardian counter types')
    exit_code = sc.get('runner_exit_code')
    require(type(exit_code) is int and type(tc.get('direct_exit_code')) is int and type(g.get('exit_code')) is int,
            'missing/malformed process exit')
    require(core['evidence_complete'] is True and not core['evidence_errors']
            and not core.get('execution_errors') and not core['runner_signals'], 'Runner evidence incomplete')
    target_ok = core['primary']['outcome'] == 'command_success' and tc['direct_exit_code'] == 0 and not core['stop_escalation']
    if target_ok:
        require(core['primary']['waitid_code'] == os.CLD_EXITED and core['primary']['waitid_status'] == 0,
                'target waitid success')
    require(core['overall_success'] is target_ok, 'contradictory core success')
    require(boot['interface_complete'] is True and boot['error'] is None
            and boot['execution_succeeded'] is target_ok and boot['accepted'] is target_ok, 'contradictory Bootstrap success')
    require(exit_code == (0 if target_ok else 1), 'Bootstrap exit/outcome contradiction')
    pre = s['pre_release_live_sample']
    trace = [b.decode(line) for line in raw['supervised/resources.jsonl'].splitlines()]
    live = [r for r in trace if r.get('valid') is True and r.get('live') is True]
    before = [r for r in live if r['classification'] == 'pre_release']
    after = [r for r in live if r['classification'] == 'post_release']
    require(before and encoded_equal(before[-1], pre), 'Supervisor raw readiness sample')
    require(s['readiness_rss_bytes'] == before[-1]['rss_bytes']
            and s['all_live_sampled_peak_bytes'] == max(r['rss_bytes'] for r in live)
            and s['post_release_live_samples'] == len(after)
            and s['post_release_sampled_peak_bytes'] == max((r['rss_bytes'] for r in after), default=None),
            'Supervisor sampled peak agreement')
    require(s['observation_complete'] is True and pre['valid'] is True and pre['live'] is True
            and events.identity(pre['identity']) == target_id and pre['end_ns'] <= s['release_begin_ns'], 'Supervisor readiness evidence')
    expected_success = target_ok and exit_code == 0 and s['reason'] is None and not s['errors']
    require(s['g4_interface_complete'] is True and s['success'] is expected_success
            and g['exit_code'] == (0 if expected_success else 1), 'contradictory Supervisor success')
    if crypto:
        from .g4_crypto import evidence
        evidence(raw, m, report['crypto'])
        require(type(g.get('outer_seconds')) is int
                and g['outer_seconds'] == m['crypto']['guardian_seconds'], 'crypto Guardian budget')
    report.update(execution_succeeded=expected_success, evidence_valid=True,
                  workload_coverage=rss['workload_coverage'], final_accepted=expected_success,
                  bootstrap_exit_code=exit_code, target=target_id, runner=runner_id, supervisor=supervisor_id,
                  chronology=[a[0]['monotonic_ns'], rows[0]['start_ns'], rows[0]['end_ns'], a[1]['monotonic_ns'],
                              a[2]['monotonic_ns'], a[3]['monotonic_ns'], rows[1]['start_ns']],
                  rss_audit=rss, resource_policy=resource, actual_lifetime_peak_bytes=None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('out')
    args = parser.parse_args()
    result = audit(args.out)
    print(json.dumps(result, sort_keys=True))
    return 0 if result['final_accepted'] else 1 if result['evidence_valid'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
