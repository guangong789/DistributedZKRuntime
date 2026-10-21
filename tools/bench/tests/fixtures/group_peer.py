"""One optional cooperative child, file barrier and raw-sample acknowledgement.

No session escape, signals, crypto or allocations. Only private test files.
"""
import json
import os
from pathlib import Path
import select
import sys
import time

mode, control, raw = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
child = writer = None
if mode == 'child':
    reader, writer = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(writer)
        os.read(reader, 1)
        os.close(reader)
        os._exit(0)
    os.close(reader)
with (control / 'ready.json').open('x') as stream:
    json.dump(dict(pid=os.getpid(), child=child), stream)
deadline = time.monotonic() + 1.5
try:
    while not (control / 'release').exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('release deadline')
        select.select([], [], [], .005)
    with (control / 'ack.json').open('x') as stream:
        json.dump(dict(pid=os.getpid(), monotonic_ns=time.monotonic_ns()), stream)
    # Work/exit is gated by a recorded complete sample, not a guessed sleep.
    while True:
        rows = []
        if raw.exists():
            with raw.open('rb') as stream:
                data = stream.read(256 * 1024)
            rows = [json.loads(line) for line in data.splitlines(keepends=True) if line.endswith(b'\n')]
        required = {os.getpid()} | ({child} if child else set())
        if any(r['phase']['name'] == 'workload' and r['sample_status'] == 'COMPLETE'
               and required <= {m['before']['pid'] for m in r['members'] if m['status'] == 'LIVE'} for r in rows):
            print('G3_SYNTHETIC_WORK_OBSERVED', flush=True)
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('workload sample deadline')
        select.select([], [], [], .005)
finally:
    if writer is not None:
        os.close(writer)
        os.waitpid(child, 0)
