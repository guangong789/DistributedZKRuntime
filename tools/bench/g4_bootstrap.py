"""Explicit G4 Runner entrypoint. The approved Runner alone owns target lifecycle."""
import argparse
import os
from pathlib import Path
import sys
import time

from . import g4_binding as binding, g4_events as events
from . import group_audit, group_rss, policy_binding as b, run_check


def runtime(m, argv, environment, cwd):
    binding.require(argv == m['runner_argv'], 'G4 runtime argv mismatch')
    allowed = set(m['environment']) | {'DZKR_MIMCCHAIN_BARRIER_SOCKET', 'DZKR_MIMCCHAIN_BARRIER_TOKEN'}
    binding.require(cwd == m['cwd'] and set(environment) == allowed
                    and all(environment.get(k) == v for k, v in m['environment'].items()),
                    'G4 runtime environment mismatch')
    binding.require(environment.get('DZKR_MIMCCHAIN_BARRIER_SOCKET')
                    and len(environment.get('DZKR_MIMCCHAIN_BARRIER_TOKEN', '')) == 64,
                    'G4 target barrier unavailable')


class Monitor:
    def __init__(self, m, link, recorder):
        self.m, self.link, self.recorder = m, link, recorder
        self.owner = self.phase = self.error = None

    def __call__(self, pgid):
        try:
            if self.owner is None:
                self.owner = b.decode(b.read_regular(Path(self.m['paths']['inner']) / 'owner.json'))
                self.link.state.bind(events.identity(self.owner))
            events.require(self.owner['pid'] == pgid, 'G4 monitor owner mismatch')
            if self.link.state.sequence == 0:
                value = self.link.receive('ready_verified')
                self.phase = dict(name='readiness', event=dict(kind='ready_verified',
                                  owner=value['owner'], monotonic_ns=value['monotonic_ns']))
            elif self.link.state.sequence == 2:
                self.link.receive('release_initiated')
                value = self.link.receive('release_acknowledged')
                self.phase = dict(name='workload', event=dict(kind='release_acknowledged',
                                  owner=value['owner'], monotonic_ns=value['monotonic_ns']))
            events.require(self.phase is not None, 'missing verified sampling phase')
            row = group_rss.observe(self.owner, self.m['run_id'], self.recorder.count, self.phase)
            self.recorder.append(row)
            events.require(row['sample_status'] in ('COMPLETE', 'ZOMBIE_ONLY', 'NO_LIVE_MEMBERS')
                           and type(row['mem_available_bytes']) is int, 'incomplete G4 monitor observation')
            if self.link.state.sequence in (1, 4):
                events.require(row['sample_status'] == 'COMPLETE', 'missing complete barrier sample')
                os.fsync(self.recorder.stream.fileno())
                event = 'readiness_persisted' if self.link.state.sequence == 1 else 'workload_persisted'
                self.link.send(event, events.identity(self.owner), sample=events.receipt(self.recorder, row))
            # This exact recorded sample is the Runner's resource-decision input.
            return dict(live=[x['after'] for x in row['members'] if x['status'] == 'LIVE'],
                        zombies=[x['after'] for x in row['members'] if x['status'] == 'ZOMBIE'], errors=[],
                        rss_kib=row['group_rss_bytes'] // 1024 if row['sample_status'] == 'COMPLETE' else 0), row['mem_available_bytes'] // 1024
        except (OSError, ValueError, TypeError, KeyError) as exc:
            self.error = type(exc).__name__
            self.link.fail('capture_or_coordination_failure')
            raise


def execute(m, link):
    """No subprocess loop here: use the approved run_command callable seam."""
    recorder = group_rss.Recorder(m['paths']['raw'], m['run_id'])
    monitor = Monitor(m, link, recorder)
    result = None
    try:
        result = run_check.run_command(m['target_argv'], m['paths']['inner'],
                    label='p4b2-rss-barrier', cwd=m['cwd'], wall_seconds=m['effective_limits']['wall_seconds'],
                    term_grace=.15, kill_grace=.5, monitor=monitor)
        if monitor.owner is not None and not recorder.error:
            phase = dict(name='cleanup', event=dict(kind='cleanup_completed',
                         monotonic_ns=time.monotonic_ns(), owner=events.identity(monitor.owner)))
            recorder.append(group_rss.observe(monitor.owner, m['run_id'], recorder.count,
                                             phase, site='cleanup_check'))
    finally:
        summary = recorder.close(result['sampled_group_peak_rss_kib'] * 1024 if result else None)
        b.write_private(Path(m['paths']['summary']), b.encoded(summary))
    audit, error = None, None
    try:
        audit = group_audit.audit(m['paths']['raw'], m['paths']['summary'])
    except (OSError, ValueError, TypeError, KeyError) as exc:
        error = type(exc).__name__
    complete = bool(audit and monitor.error is None and link.state.sequence == 5 and not link.state.failed
                    and audit['phases']['readiness']['complete'] > 0
                    and audit['phases']['workload']['complete'] > 0
                    and not audit['has_partial'] and summary['storage_complete'])
    outcome = dict(contract=binding.CONTRACT, run_id=m['run_id'],
                   execution_succeeded=result['overall_success'], interface_complete=complete,
                   accepted=complete and result['overall_success'], audit=audit,
                   error=error or monitor.error, supervisor_owner=link.peer_owner,
                   full_g4_evidence_assessed=False)
    b.write_private(Path(m['paths']['supervised']) / 'bootstrap.json', b.encoded(outcome))
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    link = None
    m = None
    stage = 'binding'
    try:
        m = binding.check(args.config, live=True)
        runtime(m, sys.orig_argv, os.environ, os.getcwd())
        stage = 'channel'
        with (Path(m['paths']['supervised']) / 'bootstrap-events.jsonl').open('x') as journal:
            link = events.connect(m, time.monotonic_ns() + int(m['effective_limits']['barrier_seconds'] * 1e9), journal)
            try:
                stage = 'execution'
                result = execute(m, link)
            finally:
                link.close()
        return 0 if result['accepted'] else 1 if result['interface_complete'] else 2
    except (OSError, ValueError, TypeError, KeyError) as exc:
        if m is not None:
            try:
                b.write_private(Path(m['paths']['supervised']) / 'bootstrap-error.json', b.encoded(
                    dict(contract=binding.CONTRACT, run_id=m['run_id'], state='failed',
                         stage=stage, error=type(exc).__name__, accepted=False)))
            except (OSError, ValueError):
                pass  # Failure to record failure can never produce a zero exit.
        print('G4 bootstrap rejected: ' + type(exc).__name__, file=sys.stderr)
        return 2


if __name__ == '__main__':
    os.umask(0o077)
    sys.exit(main())
