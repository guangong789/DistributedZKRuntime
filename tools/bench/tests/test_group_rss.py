"""Pure G3 observations and adversarial evidence; never start a process."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import sys
import unittest
from unittest import mock

from tools.bench import group_audit as audit, group_rss as rss
from tools.bench.group_schema import EvidenceError, MAX_RECORD_BYTES

RUN_ID = 'a' * 32
OWNER = dict(pid=100, start_ticks=7, pgid=100, sid=100)


def process(pid=100, **fields):
    return {**OWNER, 'state': 'R', 'pid': pid, **fields}


def row(sequence=0, amounts=(1024,), phase='unknown'):
    identities = [process(100 + n) for n in range(len(amounts))]
    event = None if phase == 'unknown' else dict(kind=dict(readiness='ready_verified',
             workload='release_acknowledged', cleanup='cleanup_completed')[phase],
             monotonic_ns=1 if phase == 'readiness' else 99 + sequence * 10, owner=OWNER)
    return dict(schema_version=1, run_id=RUN_ID, sequence=sequence,
                start_ns=100 + sequence * 10, end_ns=102 + sequence * 10,
                phase=dict(name=phase, event=event), site='runner_monitor', owner=OWNER,
                anchor_before=process(), anchor_after=process(),
                scan_before=identities, scan_after=copy.deepcopy(identities), scan_errors=[],
                members=[dict(before=p, after=copy.deepcopy(p), rss_bytes=n, status='LIVE')
                         for p, n in zip(identities, amounts)], membership_scan_status='complete',
                sample_status='COMPLETE', group_rss_bytes=sum(amounts),
                mem_available_bytes=4 * 1024**3, sampling_errors=[])


class FakeProc:
    def __init__(self):
        self.values = {100: process(), 101: process(101)}
        self.rss_errors = {}

    def pids(self):
        return list(self.values)

    def identity(self, pid):
        return self.values.get(pid)

    def rss(self, pid):
        if pid in self.rss_errors:
            raise self.rss_errors[pid]
        return 1024 * (pid - 99)

    def available(self):
        return 4 * 1024**3


class GroupRSS(unittest.TestCase):
    def setUp(self):
        retained = os.environ.get('DZKR_BENCH_TEST_OUT')
        if retained:
            self.work = Path(retained) / self._testMethodName
            self.work.mkdir(mode=0o700)
        else:
            temp = tempfile.TemporaryDirectory(prefix='dzkr-g3-pure-')
            self.addCleanup(temp.cleanup)
            self.work = Path(temp.name)
        self.raw, self.summary = self.work / 'raw.jsonl', self.work / 'summary.json'
        for target in ('subprocess.Popen', 'os.killpg', 'signal.pidfd_send_signal', 'ctypes.CDLL'):
            patch = mock.patch(target, create=True, side_effect=AssertionError('process action in pure G3 test'))
            patch.start()
            self.addCleanup(patch.stop)

    def bundle(self, records, legacy=None):
        recorder = rss.Recorder(self.raw, RUN_ID)
        for record in records:
            recorder.append(record)
        summary = recorder.close(legacy)
        self.summary.write_text(json.dumps(summary))
        if legacy is not None:
            (self.work / 'inner').mkdir()
            (self.work / 'inner/result.json').write_text(json.dumps(dict(owner=OWNER, sampled_group_peak_rss_kib=legacy // 1024)))
        return summary

    def replace_raw(self, records):
        # Preserve all expected totals/peaks; rebind only raw bytes/hash so a
        # negative control must fail on its semantic invariant, not stale SHA.
        raw = b''.join((json.dumps(r) + '\n').encode() for r in records)
        self.raw.write_bytes(raw)
        summary = json.loads(self.summary.read_text())
        summary['raw'] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        self.summary.write_text(json.dumps(summary))

    def invalid(self, records, reason):
        self.replace_raw(records)
        with self.assertRaisesRegex(EvidenceError, reason):
            audit.audit(self.raw, self.summary)

    def invalid_semantics(self, label, records, reason):
        self.invalid(records, reason)
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, 'argv', ['group_audit', str(self.raw), str(self.summary)]), \
                mock.patch.object(sys, 'stdout', stdout), mock.patch.object(sys, 'stderr', stderr):
            code = audit.main()
        self.assertEqual(code, 2)
        self.assertRegex(stdout.getvalue(), reason)
        self.assertIn('invalid G3 resource evidence:', stdout.getvalue())
        self.assertNotIn('"contract_valid": true', stdout.getvalue())
        self.assertNotIn('Traceback', stdout.getvalue())
        self.assertEqual(stderr.getvalue(), '')
        # Retained alongside optional pure fixtures; no process/CLI launch.
        with (self.work / 'semantic-rejections.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(case=label, reason=reason, records=records,
                         summary=json.loads(self.summary.read_text()), exit_code=code,
                         stdout=stdout.getvalue(), stderr=stderr.getvalue())) + '\n')

    def test_single_and_multiple_members_and_peak(self):
        self.bundle([row(0), row(1, (2048, 4096)), row(2, (1024, 1024))], legacy=6144)
        result = audit.audit(self.raw, self.summary)
        self.assertEqual(result['complete_sample_peak_bytes'], 6144)
        self.assertEqual(result['runner_monitor_peak_bytes'], 6144)
        self.assertEqual(result['samples'], 3)
        self.assertTrue(result['all_samples_complete'])
        self.assertEqual(result['workload_coverage'], 'unobserved')

    def test_per_sample_total_not_trusted(self):
        self.bundle([row(amounts=(1024, 2048))])
        bad = row(amounts=(1024, 2048))
        bad['group_rss_bytes'] = 9999
        self.invalid([bad], 'group RSS sum disagreement')

    def test_duplicate_and_missing_members(self):
        self.bundle([row(amounts=(1024, 2048))])
        duplicate = row(amounts=(1024, 2048))
        duplicate['members'].append(copy.deepcopy(duplicate['members'][0]))
        self.invalid([duplicate], 'duplicate member')
        missing = row(amounts=(1024, 2048))
        missing['members'].pop()
        self.invalid([missing], 'missing discovered member')

    def test_duplicate_scan_identity(self):
        self.bundle([row()])
        bad = row()
        bad['scan_before'].append(copy.deepcopy(bad['scan_before'][0]))
        self.invalid([bad], 'duplicate member PID')

    def test_pid_reuse_across_samples(self):
        self.bundle([row(), row(1)])
        bad = row(1)
        # The owned leader is retained by WNOWAIT; reuse of a child is also
        # conservatively outside the short G3 v1 stream identity contract.
        initial, bad = row(0, (1024, 2048)), row(1, (1024, 2048))
        for field in ('scan_before', 'scan_after'):
            bad[field][1]['start_ticks'] = 8
        for field in ('before', 'after'):
            bad['members'][1][field]['start_ticks'] = 8
        self.invalid([initial, bad], 'PID reused across samples')

    def test_member_identity_mismatch_within_sample(self):
        self.bundle([row()])
        bad = row()
        bad['members'][0]['after']['start_ticks'] = 8
        self.invalid([bad], 'identity changed during measurement')

    def test_wrong_group_or_session(self):
        self.bundle([row()])
        for field in ('pgid', 'sid'):
            bad = row()
            bad['scan_before'][0][field] = 999
            self.invalid([bad], 'member PGID/SID')

    def test_anchor_identity_mismatch(self):
        self.bundle([row()])
        bad = row()
        bad['anchor_after']['start_ticks'] = 8
        self.invalid([bad], 'anchor identity mismatch')

    def test_unreadable_and_vanished_members_are_partial(self):
        for n, error in enumerate((PermissionError(), FileNotFoundError())):
            reader = FakeProc()
            reader.rss_errors[101] = error
            result = rss.observe(OWNER, RUN_ID, n, reader=reader)
            self.assertEqual(result['sample_status'], 'PARTIAL')
            self.assertIsNone(result['group_rss_bytes'])
            self.assertEqual(result['members'][1]['status'], ('UNREADABLE', 'VANISHED')[n])
            self.assertEqual(result['members'][0]['rss_bytes'], 1024)
        reader = FakeProc()
        reader.rss_errors[101] = PermissionError()
        result = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.bundle([result])
        self.assertTrue(audit.audit(self.raw, self.summary)['has_partial'])

    def test_partial_cannot_claim_complete(self):
        reader = FakeProc()
        reader.rss_errors[101] = PermissionError()
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.bundle([observed])
        observed['sample_status'] = 'COMPLETE'
        observed['group_rss_bytes'] = 1024
        self.invalid([observed], 'sample status contradiction')

    def test_scan_error_is_not_empty_group(self):
        reader = FakeProc()
        reader.pids = mock.Mock(side_effect=PermissionError())
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.assertEqual(observed['sample_status'], 'PARTIAL')
        self.assertTrue(observed['scan_errors'])
        self.bundle([observed])
        self.assertTrue(audit.audit(self.raw, self.summary)['has_partial'])

    def test_member_joins_between_scans(self):
        reader = FakeProc()
        reader.pids = mock.Mock(side_effect=[[100], [100, 101]])
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.assertEqual(observed['members'][1]['status'], 'MISSING')
        self.assertEqual(observed['sample_status'], 'PARTIAL')
        self.bundle([observed])
        self.assertTrue(audit.audit(self.raw, self.summary)['has_partial'])

    def test_zombie_only_not_active_memory(self):
        reader = FakeProc()
        reader.values = {100: process(state='Z')}
        reader.rss = mock.Mock(side_effect=AssertionError('zombie RSS read'))
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.assertEqual(observed['sample_status'], 'ZOMBIE_ONLY')
        reader.rss.assert_not_called()
        self.bundle([observed])
        result = audit.audit(self.raw, self.summary)
        self.assertIsNone(result['complete_sample_peak_bytes'])

    def test_verified_no_live_members(self):
        reader = FakeProc()
        reader.values = {}
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.assertEqual(observed['sample_status'], 'NO_LIVE_MEMBERS')
        self.bundle([observed])
        self.assertIsNone(audit.audit(self.raw, self.summary)['complete_sample_peak_bytes'])

    def test_invalid_rss_types_and_negative(self):
        self.bundle([row()])
        for value in (True, False, -1, 1.0, '1024', None):
            bad = row()
            bad['members'][0]['rss_bytes'] = value
            self.invalid([bad], 'RSS|incomplete live member')

    def test_sequence_and_time_ordering(self):
        self.bundle([row(), row(1)])
        missing = row()
        del missing['sequence']
        self.invalid([missing], 'sample sequence')
        self.invalid([row(1), row()], 'sample sequence')
        bad = row(1)
        bad['start_ns'] = 0
        self.invalid([row(), bad], 'sample time ordering')

    def test_unsupported_version_and_run_identity(self):
        self.bundle([row()])
        for value in (2, True, None):
            bad = row()
            bad['schema_version'] = value
            self.invalid([bad], 'unsupported sample version')
        bad = row()
        bad['run_id'] = 'b' * 32
        self.invalid([bad], 'run identity')

    def test_corrupt_and_missing_summary(self):
        summary = self.bundle([row()])
        summary['complete_sample_peak_bytes'] = 999
        self.summary.write_text(json.dumps(summary))
        with self.assertRaisesRegex(EvidenceError, 'summary disagreement: complete_sample_peak_bytes'):
            audit.audit(self.raw, self.summary)
        self.summary.unlink()
        with self.assertRaises(FileNotFoundError):
            audit.audit(self.raw, self.summary)

    def test_readiness_and_workload_separation(self):
        self.bundle([row(0, (4096,), 'readiness'), row(1, (1024,), 'workload')], legacy=4096)
        result = audit.audit(self.raw, self.summary)
        self.assertEqual(result['phases']['readiness']['peak_bytes'], 4096)
        self.assertEqual(result['phases']['workload']['peak_bytes'], 1024)
        self.assertEqual(result['workload_coverage'], 'all_samples_complete_not_lifetime_peak')

    def test_missing_workload_and_barrier_event(self):
        self.bundle([row(0, phase='readiness')])
        self.assertEqual(audit.audit(self.raw, self.summary)['workload_coverage'], 'unobserved')
        bad = row(phase='workload')
        bad['phase']['event'] = None
        self.invalid([bad], 'missing explicit phase event')

    def test_partial_workload_coverage(self):
        first, second = row(0, phase='readiness'), row(1, phase='workload')
        second['members'][0].update(status='UNREADABLE', after=None, rss_bytes=None)
        second.update(sample_status='PARTIAL', group_rss_bytes=None)
        self.bundle([first, second])
        result = audit.audit(self.raw, self.summary)
        self.assertEqual(result['workload_coverage'], 'partial')
        self.assertIsNone(result['phases']['workload']['peak_bytes'])

    def test_malformed_jsonl_truncation_and_overflow(self):
        self.bundle([row()])
        for raw, reason in ((b'{\n', 'malformed JSON'), (b'{}', 'truncated'),
                            (b' ' * (MAX_RECORD_BYTES + 1), 'size/sample bound')):
            self.raw.write_bytes(raw)
            with self.assertRaisesRegex(EvidenceError, reason):
                audit.audit(self.raw, self.summary)

    def test_storage_bound_and_output_failure(self):
        recorder = rss.Recorder(self.raw, RUN_ID, max_bytes=1)
        with self.assertRaisesRegex(EvidenceError, 'storage_failure'):
            recorder.append(row())
        summary = recorder.close()
        self.assertFalse(summary['storage_complete'])
        self.summary.write_text(json.dumps(summary))
        with self.assertRaisesRegex(EvidenceError, 'incomplete evidence storage'):
            audit.audit(self.raw, self.summary)
        recorder = rss.Recorder(self.work / 'other.jsonl', RUN_ID)
        recorder.stream.close()
        with self.assertRaisesRegex(EvidenceError, 'storage_failure'):
            recorder.append(row())
        self.assertFalse(recorder.close()['storage_complete'])

    def test_runner_peak_comparison_is_same_event_only(self):
        self.bundle([row()], legacy=2048)
        with self.assertRaisesRegex(EvidenceError, 'same-event Runner peak disagreement'):
            audit.audit(self.raw, self.summary)

    def test_published_runner_peak_not_summary_claim(self):
        self.bundle([row()], legacy=1024)
        (self.work / 'inner/result.json').write_text(json.dumps(dict(owner=OWNER, sampled_group_peak_rss_kib=99)))
        with self.assertRaisesRegex(EvidenceError, 'published Runner peak disagreement'):
            audit.audit(self.raw, self.summary)

    def test_observer_member_identity_race_is_invalid(self):
        reader = FakeProc()
        real = reader.identity
        counts = {}
        def identity(pid):
            counts[pid] = counts.get(pid, 0) + 1
            return process(101, start_ticks=999) if pid == 101 and counts[pid] == 2 else real(pid)
        reader.identity = identity
        observed = rss.observe(OWNER, RUN_ID, 0, reader=reader)
        self.assertEqual(observed['sample_status'], 'INVALID')
        self.bundle([observed])
        with self.assertRaisesRegex(EvidenceError, 'identity changed during measurement'):
            audit.audit(self.raw, self.summary)

    def test_missing_vmrss_is_never_zero(self):
        reader = rss.ProcReader()
        with mock.patch.object(reader, '_read', return_value='Name:\ttest\n'):
            with self.assertRaisesRegex(ValueError, 'missing or malformed VmRSS'):
                reader.rss(100)

    def test_sample_count_bound(self):
        recorder = rss.Recorder(self.raw, RUN_ID, max_samples=1)
        recorder.append(row())
        with self.assertRaisesRegex(EvidenceError, 'storage_failure'):
            recorder.append(row(1))
        summary = recorder.close()
        self.assertEqual(summary['samples'], 1)
        self.assertFalse(summary['storage_complete'])

    def test_phase_chronology_rejections(self):
        original = [row(0, phase='readiness'), row(1, phase='workload'), row(2, phase='cleanup')]
        self.bundle(original)
        self.assertTrue(audit.audit(self.raw, self.summary)['contract_valid'])
        for label, index, timestamp, reason in (
                ('original-ack-before-readiness', 1, 1, 'phase chronology: acknowledgement precedes readiness end'),
                ('ack-during-readiness', 1, 101, 'phase chronology: acknowledgement precedes readiness end'),
                ('workload-before-ack', 1, 111, 'phase event identity/time'),
                ('cleanup-before-workload-end', 2, 111, 'phase chronology: cleanup precedes prior observation end')):
            with self.subTest(case=label):
                bad = copy.deepcopy(original)
                bad[index]['phase']['event']['monotonic_ns'] = timestamp
                self.invalid_semantics(label, bad, reason)
        bad = copy.deepcopy(original)
        bad[2]['phase'] = dict(name='readiness', event=dict(kind='ready_verified', monotonic_ns=119, owner=OWNER))
        self.invalid_semantics('reversed-phase-transition', bad, 'phase order reversed')

    def test_phase_chronology_inclusive_boundaries(self):
        original = [row(0, phase='readiness'), row(1, phase='workload'), row(2, phase='cleanup')]
        self.bundle(original)
        for ack, cleanup in ((103, 113), (102, 112), (110, 120)):
            with self.subTest(ack=ack, cleanup=cleanup):
                valid = copy.deepcopy(original)
                valid[1]['phase']['event']['monotonic_ns'] = ack
                valid[2]['phase']['event']['monotonic_ns'] = cleanup
                self.replace_raw(valid)
                result = audit.audit(self.raw, self.summary)
                self.assertEqual(result['workload_coverage'], 'all_samples_complete_not_lifetime_peak')
                self.assertEqual(result['complete_sample_peak_bytes'], 1024)

    def test_unknown_intervals_do_not_invent_phase_boundaries(self):
        records = [row(0, phase='readiness'), row(1), row(2, phase='workload')]
        # Ack may precede an intervening unknown observation; that observation
        # makes no claim to be wholly pre-release or post-release.
        records[2]['phase']['event']['monotonic_ns'] = 103
        self.bundle(records)
        result = audit.audit(self.raw, self.summary)
        self.assertEqual(result['phases']['unknown']['samples'], 1)
        self.assertEqual(result['phases']['workload']['complete'], 1)

    def test_phase_events_can_precede_repeated_same_phase_samples(self):
        records = [row(0, phase='readiness'), row(1, phase='readiness'),
                   row(2, phase='workload'), row(3, phase='workload'),
                   row(4, phase='cleanup'), row(5, phase='cleanup')]
        records[3]['phase']['event']['monotonic_ns'] = records[2]['phase']['event']['monotonic_ns']
        records[5]['phase']['event']['monotonic_ns'] = records[4]['phase']['event']['monotonic_ns']
        self.bundle(records)
        result = audit.audit(self.raw, self.summary)
        self.assertTrue(result['all_samples_complete'])
        self.assertEqual(result['phases']['workload']['complete'], 2)
        bad = copy.deepcopy(records)
        bad[3]['phase']['event']['monotonic_ns'] -= 1
        self.invalid_semantics('reversed-repeated-phase-event', bad, 'phase chronology: event time reversed')

    def test_discovery_state_rejections(self):
        original = row()
        self.bundle([original])
        self.assertTrue(audit.audit(self.raw, self.summary)['contract_valid'])
        missing = object()
        for label, state, reason in (
                ('zombie-discovery-live-member', 'Z', 'member discovery state mismatch'),
                ('different-live-discovery-copy', 'S', 'member discovery state mismatch'),
                ('missing-opening-state', missing, 'missing scan state'),
                ('boolean-opening-state', True, 'process state'),
                ('null-opening-state', None, 'process state')):
            with self.subTest(case=label):
                bad = copy.deepcopy(original)
                discovery = dict(bad['scan_before'][0])
                if state is missing:
                    del discovery['state']
                else:
                    discovery['state'] = state
                # Replace only the scan record; row() intentionally shares
                # member.before with its discovery observation.
                bad['scan_before'][0] = discovery
                self.invalid_semantics(label, [bad], reason)
        bad = copy.deepcopy(original)
        bad['scan_before'][0] = dict(bad['scan_before'][0], start_ticks=8)
        self.invalid_semantics('discovery-identity-mismatch', [bad], 'member discovery identity mismatch')

    def test_zombie_discovery_rejections(self):
        original = row()
        zombie = process(state='Z')
        original.update(anchor_before=zombie, anchor_after=zombie,
                        scan_before=[zombie], scan_after=[zombie],
                        members=[dict(before=zombie, after=zombie, rss_bytes=None, status='ZOMBIE')],
                        sample_status='ZOMBIE_ONLY', group_rss_bytes=None)
        self.bundle([original])
        self.assertEqual(audit.audit(self.raw, self.summary)['status_counts']['ZOMBIE_ONLY'], 1)
        for label, location, reason in (
                ('live-discovery-zombie-member', 'opening', 'member discovery state mismatch'),
                ('zombie-discovery-live-before', 'before', 'member discovery state mismatch'),
                ('zombie-then-live-after', 'after', 'member state resurrection'),
                ('zombie-then-live-closing', 'closing', 'closing discovery state resurrection')):
            with self.subTest(case=label):
                bad = copy.deepcopy(original)
                live = process()
                if location == 'opening':
                    bad['scan_before'] = [live]
                elif location == 'closing':
                    bad['scan_after'] = [live]
                else:
                    bad['members'][0][location] = live
                self.invalid_semantics(label, [bad], reason)

    def test_distinct_state_observations_remain_valid(self):
        for label, first, after, closing, status in (
                ('running-to-sleeping', 'R', 'S', 'S', 'COMPLETE'),
                ('stopped-to-running', 'T', 'R', 'S', 'COMPLETE'),
                ('exit-after-live-measurement', 'R', 'S', 'Z', 'COMPLETE'),
                ('exit-during-measurement', 'R', 'Z', 'Z', 'PARTIAL')):
            with self.subTest(case=label):
                valid = row()
                valid['scan_before'] = [process(state=first)]
                valid['scan_after'] = [process(state=closing)]
                valid['anchor_before'], valid['anchor_after'] = process(state=first), process(state=closing)
                valid['members'][0].update(before=process(state=first), after=process(state=after),
                                           status='LIVE' if status == 'COMPLETE' else 'VANISHED')
                valid['sample_status'] = status
                valid['group_rss_bytes'] = 1024 if status == 'COMPLETE' else None
                work = self.work / label
                work.mkdir()
                recorder = rss.Recorder(work / 'raw.jsonl', RUN_ID)
                recorder.append(valid)
                (work / 'summary.json').write_text(json.dumps(recorder.close()))
                result = audit.audit(work / 'raw.jsonl', work / 'summary.json')
                self.assertEqual(result['status_counts'][status], 1)
                self.assertEqual(result['complete_sample_peak_bytes'], 1024 if status == 'COMPLETE' else None)

    def test_missing_member_uses_closing_discovery_state(self):
        original = row()
        child = process(101, state='S')
        original['scan_after'].append(child)
        original['members'].append(dict(before=child, after=None, rss_bytes=None, status='MISSING'))
        original.update(sample_status='PARTIAL', group_rss_bytes=None)
        self.bundle([original])
        self.assertTrue(audit.audit(self.raw, self.summary)['has_partial'])
        bad = copy.deepcopy(original)
        bad['members'][1]['before'] = dict(child, state='Z')
        self.invalid_semantics('missing-member-closing-state-copy', [bad], 'member discovery state mismatch')


if __name__ == '__main__':
    unittest.main()
