"""Independent, bounded G3 JSONL auditor. No observer/Runner imports or processes."""
import argparse
import hashlib
import json
from pathlib import Path
import re

from .group_schema import (CONTRACT, VERSION, KEYS, MAX_MEMBERS, MAX_PROC_ENTRIES, MAX_RECORD_BYTES,
                           MAX_STREAM_BYTES, MAX_SAMPLES, MAX_RSS_BYTES, PHASES, STATUSES, EvidenceError)


def require(condition, reason):
    if not condition:
        raise EvidenceError(reason)


def decode(raw):
    def pairs(items):
        result = {}
        for k, v in items:
            require(k not in result, 'duplicate JSON key')
            result[k] = v
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False, 'nonfinite JSON'))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise EvidenceError('malformed JSON: ' + type(exc).__name__) from exc


def identity(value):
    require(isinstance(value, dict), 'identity mapping')
    require(all(type(value.get(k)) is int and value[k] > 1 for k in ('pid', 'pgid', 'sid'))
            and type(value.get('start_ticks')) is int and value['start_ticks'] >= 0, 'identity integer fields')
    if 'state' in value:
        require(isinstance(value['state'], str) and re.fullmatch('[A-Za-z]', value['state']) is not None, 'process state')
    return tuple(value[k] for k in KEYS)


def sample(row, owner, sequence, last_end, run_id, seen):
    require(isinstance(row, dict) and type(row.get('schema_version')) is int
            and row['schema_version'] == VERSION, 'unsupported sample version')
    require(row.get('run_id') == run_id, 'run identity')
    require(type(row.get('sequence')) is int and row['sequence'] == sequence, 'sample sequence')
    require(type(row.get('start_ns')) is int and type(row.get('end_ns')) is int
            and 0 <= row['start_ns'] <= row['end_ns'] and row['start_ns'] >= last_end, 'sample time ordering')
    expected = identity(owner)
    require(owner['pid'] == owner['pgid'] == owner['sid'], 'owned session anchor')
    require(identity(row.get('owner')) == expected, 'group identity changed')
    for anchor in ('anchor_before', 'anchor_after'):
        require(anchor in row, 'missing anchor observation')
        if row[anchor] is not None:
            require(identity(row[anchor]) == expected, 'anchor identity mismatch')
    scans, scan_records = [], []
    for name in ('scan_before', 'scan_after'):
        values = row.get(name)
        require(isinstance(values, list) and len(values) <= MAX_MEMBERS, 'membership scan bound/shape')
        found, records = {}, {}
        for value in values:
            key = identity(value)
            require('state' in value, 'missing scan state')
            require(key[0] not in found, 'duplicate member PID in scan')
            require(value['pgid'] == owner['pgid'] and value['sid'] == owner['sid'], 'member PGID/SID')
            found[key[0]] = key
            records[key[0]] = value
        scans.append(found)
        scan_records.append(records)
    require(isinstance(row.get('scan_errors'), list) and all(isinstance(x, str) for x in row['scan_errors']), 'scan errors shape')
    require(isinstance(row.get('sampling_errors'), list) and all(isinstance(x, str) for x in row['sampling_errors']), 'sampling errors shape')
    require(row.get('membership_scan_status') == ('partial' if row['scan_errors'] else 'complete'), 'scan status contradiction')
    values = row.get('members')
    require(isinstance(values, list) and len(values) <= MAX_MEMBERS, 'member bound/shape')
    members, live_rss, partial = {}, [], False
    for value in values:
        require(isinstance(value, dict), 'member mapping')
        before = value.get('before')
        key = identity(before)
        pid = before['pid']
        require('state' in before and pid not in members, 'duplicate member or missing state')
        require(key == scans[0].get(pid, scans[1].get(pid)), 'member discovery identity mismatch')
        discovered = scan_records[0].get(pid, scan_records[1].get(pid))
        require(before['state'] == discovered['state'], 'member discovery state mismatch')
        require(before['pgid'] == owner['pgid'] and before['sid'] == owner['sid'], 'member PGID/SID')
        require(pid not in seen or seen[pid] == before['start_ticks'], 'PID reused across samples')
        seen[pid] = before['start_ticks']
        require(len(seen) <= MAX_PROC_ENTRIES, 'stream identity history bound')
        require('rss_bytes' in value and 'after' in value, 'missing member measurement')
        rss, after, status = value['rss_bytes'], value['after'], value.get('status')
        require(rss is None or type(rss) is int and 0 <= rss <= MAX_RSS_BYTES, 'RSS must be nonnegative integer bytes')
        if after is not None:
            require(identity(after) == key, 'member identity changed during measurement')
            require('state' in after, 'missing after state')
            require(before['state'] not in ('Z', 'X') or after['state'] in ('Z', 'X'),
                    'member state resurrection')
        closing = scan_records[1].get(pid)
        if closing is not None and scans[1][pid] == key:
            was_dead = before['state'] in ('Z', 'X') or after is not None and after['state'] in ('Z', 'X')
            require(not was_dead or closing['state'] in ('Z', 'X'), 'closing discovery state resurrection')
        if status == 'LIVE':
            require(after is not None and type(rss) is int
                    and before['state'] not in ('Z', 'X') and after['state'] not in ('Z', 'X'), 'incomplete live member')
            require(scans[1].get(pid) == key, 'live member missing from closing scan')
            live_rss.append(rss)
        elif status == 'ZOMBIE':
            require(after is not None and before['state'] in ('Z', 'X') and after['state'] in ('Z', 'X')
                    and rss is None, 'zombie RSS/identity')
        elif status in ('MISSING', 'UNREADABLE', 'VANISHED'):
            partial = True
        else:
            raise EvidenceError('invalid member observation: ' + str(status))
        members[pid] = key
    require(set(members) == set(scans[0]) | set(scans[1]), 'missing discovered member')
    partial = (partial or bool(row['scan_errors']) or bool(row['sampling_errors'])
               or scans[0] != scans[1]
               or bool(members) and (row['anchor_before'] is None or row['anchor_after'] is None)
               or any(row[a] is not None and owner['pid'] not in scans[j]
                      for j, a in enumerate(('anchor_before', 'anchor_after'))))
    available = row.get('mem_available_bytes')
    require(available is None or type(available) is int and 0 <= available <= MAX_RSS_BYTES, 'MemAvailable integer bytes')
    if available is None:
        require('mem_available_unreadable' in row['sampling_errors'] or 'observation_interrupted' in row['sampling_errors'], 'missing MemAvailable reason')
    status = ('PARTIAL' if partial else 'NO_LIVE_MEMBERS' if not members else
              'COMPLETE' if live_rss else 'ZOMBIE_ONLY')
    require(row.get('sample_status') == status, 'sample status contradiction')
    total = None
    if status == 'COMPLETE':
        total = sum(live_rss)  # Independent formula; never calls recorder aggregation.
        require(type(row.get('group_rss_bytes')) is int and row['group_rss_bytes'] == total, 'group RSS sum disagreement')
    else:
        require(row.get('group_rss_bytes') is None, 'incomplete sample claimed complete total')
    phase = row.get('phase')
    require(isinstance(phase, dict) and phase.get('name') in PHASES, 'sampling phase')
    event = phase.get('event')
    if phase['name'] == 'unknown':
        require(event is None, 'unknown phase event')
    else:
        require(isinstance(event, dict), 'missing explicit phase event')
        expected_event = dict(readiness='ready_verified', workload='release_acknowledged', cleanup='cleanup_completed')[phase['name']]
        require(event.get('kind') == expected_event and type(event.get('monotonic_ns')) is int
                and 0 <= event['monotonic_ns'] <= row['start_ns'] and identity(event.get('owner')) == expected, 'phase event identity/time')
    require(row.get('site') in ('runner_monitor', 'cleanup_check'), 'sampling site')
    return status, total, phase['name']


def audit(raw_path, summary_path):
    summary_path = Path(summary_path)
    with summary_path.open('rb') as stream:
        summary_bytes = stream.read(MAX_RECORD_BYTES + 1)
    require(len(summary_bytes) <= MAX_RECORD_BYTES, 'summary size bound')
    summary = decode(summary_bytes)
    require(isinstance(summary, dict) and summary.get('contract') == CONTRACT
            and type(summary.get('schema_version')) is int and summary['schema_version'] == VERSION, 'summary schema')
    run_id = summary.get('run_id')
    require(isinstance(run_id, str) and re.fullmatch('[0-9a-f]{32}', run_id) is not None, 'run ID')
    require(summary.get('storage_complete') is True and summary.get('storage_error') is None, 'incomplete evidence storage')
    count, size, last, owner, seen, digest = 0, 0, -1, None, {}, hashlib.sha256()
    statuses = dict.fromkeys(STATUSES, 0)
    phases = {p: dict(samples=0, complete=0, peak_bytes=None) for p in PHASES}
    monitor_peak = max_gap = None
    last_phase_rank = -1
    last_phase_event_ns = -1
    readiness_end = cleanup_boundary = None
    with Path(raw_path).open('rb') as stream:
        while True:
            raw = stream.readline(MAX_RECORD_BYTES + 1)
            if not raw:
                break
            size += len(raw)
            require(len(raw) <= MAX_RECORD_BYTES and size <= MAX_STREAM_BYTES and count < MAX_SAMPLES, 'evidence size/sample bound')
            require(raw.endswith(b'\n'), 'truncated JSONL record')
            digest.update(raw)
            row = decode(raw)
            require(isinstance(row, dict), 'sample mapping')
            if owner is None:
                owner = row.get('owner')
            status, total, phase = sample(row, owner, count, last, run_id, seen)
            if phase != 'unknown':
                rank = PHASES.index(phase)
                event_ns = row['phase']['event']['monotonic_ns']
                require(rank >= last_phase_rank, 'phase order reversed')
                if phase == 'workload':
                    require(phases['readiness']['samples'] > 0, 'workload without readiness evidence')
                    require(event_ns >= readiness_end,
                            'phase chronology: acknowledgement precedes readiness end')
                elif phase == 'readiness':
                    readiness_end = row['end_ns']
                elif phase == 'cleanup':
                    if cleanup_boundary is None:
                        cleanup_boundary = last
                    require(event_ns >= cleanup_boundary,
                            'phase chronology: cleanup precedes prior observation end')
                require(event_ns >= last_phase_event_ns, 'phase chronology: event time reversed')
                last_phase_event_ns = event_ns
                last_phase_rank = rank
            statuses[status] += 1
            phases[phase]['samples'] += 1
            if total is not None:
                phases[phase]['complete'] += 1
                peak = phases[phase]['peak_bytes']
                phases[phase]['peak_bytes'] = total if peak is None else max(peak, total)
                if row['site'] == 'runner_monitor':
                    monitor_peak = total if monitor_peak is None else max(monitor_peak, total)
            if count:
                gap = row['start_ns'] - last
                max_gap = gap if max_gap is None else max(max_gap, gap)
            last = row['end_ns']
            count += 1
    require(count > 0, 'missing samples')
    workload = phases['workload']
    derived = dict(raw=dict(bytes=size, sha256=digest.hexdigest()), samples=count, status_counts=statuses,
                   phases=phases, runner_monitor_peak_bytes=monitor_peak, max_gap_ns=max_gap,
                   complete_sample_peak_bytes=max((x['peak_bytes'] for x in phases.values()
                                                   if x['peak_bytes'] is not None), default=None),
                   workload_coverage=('unobserved' if not workload['samples'] else
                                      'all_samples_complete_not_lifetime_peak' if workload['samples'] == workload['complete'] else 'partial'),
                   actual_lifetime_peak_bytes=None)
    encode = lambda x: json.dumps(x, sort_keys=True, allow_nan=False)
    for k, value in derived.items():
        require(k in summary and encode(summary[k]) == encode(value), 'summary disagreement: ' + k)
    legacy = summary.get('legacy_runner_peak_bytes')
    require(legacy is None or type(legacy) is int and legacy >= 0, 'legacy peak integer bytes')
    if legacy is not None:
        require(legacy == monitor_peak, 'same-event Runner peak disagreement')
        # Compare the real published Runner result, not only a summary's claim.
        with (Path(raw_path).parent / 'inner/result.json').open('rb') as stream:
            result_bytes = stream.read(2 * 1024 * 1024 + 1)
        require(len(result_bytes) <= 2 * 1024 * 1024, 'Runner result size bound')
        runner = decode(result_bytes)
        require(isinstance(runner, dict) and identity(runner.get('owner')) == identity(owner), 'Runner result owner')
        reported = runner.get('sampled_group_peak_rss_kib')
        require(type(reported) is int and reported >= 0 and reported * 1024 == legacy, 'published Runner peak disagreement')
    return dict(contract_valid=True, **derived,
                all_samples_complete=statuses['COMPLETE'] == count,
                has_partial=statuses['PARTIAL'] > 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw')
    parser.add_argument('summary')
    args = parser.parse_args()
    try:
        print(json.dumps(audit(args.raw, args.summary), sort_keys=True))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print('invalid G3 resource evidence: ' + str(exc))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
