"""Bounded Linux /proc observations and streaming G3 recording; no signaling.

Two membership scans bracket per-member identity/RSS/identity reads. This is
interval consistency, not an atomic kernel snapshot or a lifetime peak.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time

from .group_schema import (CONTRACT, VERSION, KEYS, MAX_MEMBERS, MAX_PROC_ENTRIES,
                           MAX_RECORD_BYTES, MAX_STREAM_BYTES, MAX_SAMPLES,
                           PHASES, STATUSES, EvidenceError)


class ProcReader:
    def _read(self, path):
        with Path(path).open('rb') as stream:
            data = stream.read(65537)
        if len(data) > 65536:
            raise ValueError('proc record bound')
        return data.decode('ascii')

    def pids(self):
        found = []
        with os.scandir('/proc') as entries:
            for entry in entries:
                if entry.name.isdigit():
                    found.append(int(entry.name))
                    if len(found) > MAX_PROC_ENTRIES:
                        raise ValueError('proc enumeration bound')
        return sorted(found)

    def identity(self, pid):
        try:
            raw = self._read('/proc/%d/stat' % pid)
        except (FileNotFoundError, ProcessLookupError):
            return None
        fields = raw[raw.rfind(')') + 2:].split()
        return dict(pid=pid, state=fields[0], pgid=int(fields[2]),
                    sid=int(fields[3]), start_ticks=int(fields[19]))

    def rss(self, pid):
        lines = self._read('/proc/%d/status' % pid).splitlines()
        values = [line for line in lines if line.startswith('VmRSS:')]
        if len(values) != 1 or re.fullmatch(r'VmRSS:\s+\d+\s+kB', values[0]) is None:
            raise ValueError('missing or malformed VmRSS')
        return int(values[0].split()[1]) * 1024

    def available(self):
        values = [line for line in self._read('/proc/meminfo').splitlines()
                  if line.startswith('MemAvailable:')]
        if len(values) != 1 or re.fullmatch(r'MemAvailable:\s+\d+\s+kB', values[0]) is None:
            raise ValueError('missing or malformed MemAvailable')
        return int(values[0].split()[1]) * 1024


def same(a, b):
    return a is not None and b is not None and all(a.get(k) == b.get(k) for k in KEYS)


def scan(reader, owner):
    found, errors = [], []
    try:
        pids = reader.pids()
    except (OSError, ValueError):
        return [], ['enumeration_failed']
    for pid in pids:
        try:
            item = reader.identity(pid)
            if item and item['pgid'] == owner['pgid']:
                found.append(item)
                if len(found) > MAX_MEMBERS:
                    return found[:MAX_MEMBERS], errors + ['member_bound']
        except (OSError, ValueError, IndexError):
            # Even an unreadable unidentified PID might belong to our group.
            errors.append('unreadable_pid:%d' % pid)
    return found, errors


def observe(owner, run_id, sequence, phase=None, site='runner_monitor', reader=None):
    reader = reader or ProcReader()
    phase = phase or dict(name='unknown', event=None)
    row = dict(schema_version=VERSION, run_id=run_id, sequence=sequence,
               start_ns=time.monotonic_ns(), end_ns=None, phase=phase, site=site,
               owner={k: owner[k] for k in KEYS}, anchor_before=None, anchor_after=None,
               scan_before=[], scan_after=[], scan_errors=[], members=[],
               membership_scan_status='complete', sample_status=None,
               group_rss_bytes=None, mem_available_bytes=None, sampling_errors=[])
    try:
        row['anchor_before'] = reader.identity(owner['pid'])
        before, errors = scan(reader, owner)
        row['scan_before'], row['scan_errors'] = before, errors
        for item in before:
            member = dict(before=item, after=None, rss_bytes=None, status=None)
            try:
                if item['state'] in ('Z', 'X'):
                    member['after'] = reader.identity(item['pid'])
                    member['status'] = 'ZOMBIE'
                else:
                    member['rss_bytes'] = reader.rss(item['pid'])
                    member['after'] = reader.identity(item['pid'])
                    member['status'] = 'LIVE'
                if member['after'] is None:
                    member['status'] = 'VANISHED'
                elif not same(item, member['after']):
                    member['status'] = 'IDENTITY_CHANGED'
                elif member['after']['state'] != item['state'] and member['after']['state'] in ('Z', 'X'):
                    member['status'] = 'VANISHED'
            except (FileNotFoundError, ProcessLookupError):
                member['status'] = 'VANISHED'
            except (OSError, ValueError, IndexError):
                member['status'] = 'UNREADABLE'
            row['members'].append(member)
        after, errors = scan(reader, owner)
        row['scan_after'] = after
        row['scan_errors'] += errors
        discovered = {x['pid'] for x in before}
        for item in after:
            if item['pid'] not in discovered:
                row['members'].append(dict(before=item, after=None, rss_bytes=None, status='MISSING'))
        row['anchor_after'] = reader.identity(owner['pid'])
        try:
            row['mem_available_bytes'] = reader.available()
        except (OSError, ValueError):
            row['sampling_errors'].append('mem_available_unreadable')
    except (OSError, ValueError, IndexError):
        row['sampling_errors'].append('observation_interrupted')
    row['end_ns'] = time.monotonic_ns()
    row['membership_scan_status'] = 'partial' if row['scan_errors'] else 'complete'
    anchors = (row['anchor_before'], row['anchor_after'])
    members = row['members']
    signature = lambda items: sorted(tuple(x[k] for k in KEYS) for x in items)
    if (any(a is not None and not same(a, owner) for a in anchors)
            or any(m['before']['sid'] != owner['sid'] or m['status'] == 'IDENTITY_CHANGED' for m in members)):
        status = 'INVALID'
    elif (row['scan_errors'] or row['sampling_errors']
          or signature(row['scan_before']) != signature(row['scan_after'])
          or any(m['status'] not in ('LIVE', 'ZOMBIE') for m in members)
          or any(a is not None and not any(same(a, x) for x in row[name])
                 for a, name in zip(anchors, ('scan_before', 'scan_after')))
          or (members and any(a is None for a in anchors))):
        status = 'PARTIAL'
    elif not members and all(a is None for a in anchors):
        status = 'NO_LIVE_MEMBERS'
    elif not members:
        status = 'PARTIAL'  # A visible anchor was omitted by the scans.
    elif all(m['status'] == 'ZOMBIE' for m in members):
        status = 'ZOMBIE_ONLY'
    else:
        status = 'COMPLETE'
        row['group_rss_bytes'] = sum(m['rss_bytes'] for m in members if m['status'] == 'LIVE')
    row['sample_status'] = status
    return row


class Recorder:
    def __init__(self, path, run_id, *, max_bytes=MAX_STREAM_BYTES, max_samples=MAX_SAMPLES):
        self.path, self.run_id = Path(path), run_id
        self.stream = self.path.open('xb')
        self.max_bytes, self.max_samples = min(max_bytes, MAX_STREAM_BYTES), min(max_samples, MAX_SAMPLES)
        self.bytes = self.count = 0
        self.hash = hashlib.sha256()
        self.error = None
        self.statuses = dict.fromkeys(STATUSES, 0)
        self.phases = {p: dict(samples=0, complete=0, peak_bytes=None) for p in PHASES}
        self.monitor_peak = None
        self.last_end = self.max_gap = None

    def append(self, row):
        try:
            data = (json.dumps(row, sort_keys=True, allow_nan=False) + '\n').encode()
            if (self.error or len(row['members']) > MAX_MEMBERS
                    or len(data) > MAX_RECORD_BYTES or self.bytes + len(data) > self.max_bytes
                    or self.count >= self.max_samples):
                raise EvidenceError('raw evidence storage bound')
            if self.stream.write(data) != len(data):
                raise OSError('short raw evidence write')
            self.stream.flush()
        except (OSError, ValueError) as exc:
            self.error = 'storage_failure:' + type(exc).__name__
            raise EvidenceError(self.error) from exc
        self.hash.update(data)
        self.bytes += len(data)
        self.count += 1
        self.statuses[row['sample_status']] += 1
        phase = self.phases[row['phase']['name']]
        phase['samples'] += 1
        if row['sample_status'] == 'COMPLETE':
            rss = row['group_rss_bytes']
            phase['complete'] += 1
            phase['peak_bytes'] = max(phase['peak_bytes'] or 0, rss)
            if row['site'] == 'runner_monitor':
                self.monitor_peak = max(self.monitor_peak or 0, rss)
        if self.last_end is not None:
            self.max_gap = max(self.max_gap or 0, row['start_ns'] - self.last_end)
        self.last_end = row['end_ns']

    def close(self, legacy_peak_bytes=None):
        try:
            self.stream.close()
        except OSError:
            self.error = 'storage_failure:close'
        workload = self.phases['workload']
        return dict(contract=CONTRACT, schema_version=VERSION, run_id=self.run_id,
                    raw=dict(bytes=self.bytes, sha256=self.hash.hexdigest()), samples=self.count,
                    status_counts=self.statuses, phases=self.phases,
                    complete_sample_peak_bytes=max((p['peak_bytes'] for p in self.phases.values()
                                                    if p['peak_bytes'] is not None), default=None),
                    runner_monitor_peak_bytes=self.monitor_peak,
                    legacy_runner_peak_bytes=legacy_peak_bytes, max_gap_ns=self.max_gap,
                    storage_complete=self.error is None, storage_error=self.error,
                    workload_coverage=('unobserved' if not workload['samples'] else
                                       'all_samples_complete_not_lifetime_peak' if workload['samples'] == workload['complete']
                                       else 'partial'), actual_lifetime_peak_bytes=None)
