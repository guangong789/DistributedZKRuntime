"""G3 v1 bounds and names, shared without sharing aggregation algorithms."""

VERSION = 1
CONTRACT = 'g3-raw-group-rss-v1'
MAX_MEMBERS = 256
MAX_PROC_ENTRIES = 65536
MAX_RECORD_BYTES = 256 * 1024
MAX_STREAM_BYTES = 16 * 1024 * 1024
MAX_SAMPLES = 8192
MAX_RSS_BYTES = 2**63 - 1
PHASES = ('unknown', 'readiness', 'workload', 'cleanup')
STATUSES = ('COMPLETE', 'PARTIAL', 'INVALID', 'NO_LIVE_MEMBERS', 'ZOMBIE_ONLY')
KEYS = ('pid', 'start_ticks', 'pgid', 'sid')


class EvidenceError(ValueError):
    """Invalid or incomplete resource evidence; never a request to repair it."""
