"""Non-cryptographic target: inherited PGID, readiness pipe, no timing-only ordering."""
import json
import os
from pathlib import Path
import signal
import sys
import time

mode = sys.argv[1]
if mode == "exit":
    print("stdout preserved", flush=True)
    print("stderr preserved", file=sys.stderr, flush=True)
    sys.exit(int(sys.argv[2]))

read_fd, write_fd = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(read_fd)
    if mode == "resistant":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.write(write_fd, b"ready")
    os.close(write_fd)
    while True:
        signal.pause()
os.close(write_fd)
assert os.read(read_fd, 5) == b"ready"
os.close(read_fd)
print(json.dumps(dict(event="READY", parent=os.getpid(), descendant=pid, pgid=os.getpgrp(), sid=os.getsid(0))), flush=True)
if mode in ("orphan", "resistant"):
    sys.exit(int(sys.argv[2]))
while True:
    signal.pause()
