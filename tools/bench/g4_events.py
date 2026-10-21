"""Bounded G4 control channel; no target protocol, process creation or signaling."""
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import struct
import time

from . import policy_binding as b, processes as ps
from .g4_binding import CONTRACT, run_id

ORDER = ('ready_verified', 'readiness_persisted', 'release_initiated',
         'release_acknowledged', 'workload_persisted')
STATES = ('ready', 'persisted', 'releasing', 'acknowledged', 'complete')
MAX_MESSAGE = 2048


class InterfaceError(ValueError):
    pass


def require(value, reason):
    if not value:
        raise InterfaceError(reason)


def owner(value):
    require(isinstance(value, dict) and set(value) == set(ps.KEYS), 'event owner shape')
    require(all(type(value[k]) is int and value[k] > 1 for k in ('pid', 'pgid', 'sid'))
            and value['pid'] == value['pgid'] == value['sid']
            and type(value['start_ticks']) is int and value['start_ticks'] >= 0, 'event owner identity')
    return value


def identity(value):
    require(isinstance(value, dict) and all(k in value for k in ps.KEYS), 'event identity fields')
    return owner({k: value[k] for k in ps.KEYS})


class State:
    """One globally sequenced, lockstep exchange shared by both endpoints."""
    def __init__(self, identity, clock=time.monotonic_ns):
        self.run_id = run_id(identity)
        self.clock = clock
        self.sequence, self.last_ns, self.owner = 0, -1, None
        self.ready, self.ack_ns = None, None
        self.failed = False

    def bind(self, target):
        target = owner(target)
        require(self.owner is None or self.owner == target, 'event target changed')
        self.owner = dict(target)

    def accept(self, message):
        try:
            return self._accept(message)
        except InterfaceError:
            self.failed = True
            raise

    def _accept(self, message):
        require(not self.failed, 'channel already failed')
        require(isinstance(message, dict) and set(message) ==
                {'schema', 'contract', 'run_id', 'sequence', 'event', 'owner', 'monotonic_ns', 'state', 'sample'},
                'event fields')
        require(type(message['schema']) is int and message['schema'] == 1
                and message['contract'] == CONTRACT and message['run_id'] == self.run_id, 'event run/version')
        n = message['sequence']
        require(type(n) is int and n == self.sequence and n < len(ORDER), 'event sequence/replay')
        require(message['event'] == ORDER[n] and message['state'] == STATES[n], 'event order/state')
        stamp = message['monotonic_ns']
        require(type(stamp) is int and 0 <= stamp and self.last_ns <= stamp <= self.clock(), 'event chronology')
        self.bind(message['owner'])
        sample = message['sample']
        if n in (1, 4):
            require(isinstance(sample, dict) and set(sample) ==
                    {'sequence', 'start_ns', 'end_ns', 'raw_bytes', 'raw_sha256'}, 'persisted sample fields')
            require(all(type(sample[k]) is int for k in ('sequence', 'start_ns', 'end_ns', 'raw_bytes'))
                    and sample['sequence'] == (0 if n == 1 else 1)
                    and self.last_ns <= sample['start_ns'] <= sample['end_ns'] <= stamp
                    and 0 < sample['raw_bytes'] <= 16 * 1024 * 1024
                    and isinstance(sample['raw_sha256'], str)
                    and re.fullmatch('[0-9a-f]{64}', sample['raw_sha256']), 'persisted sample chronology/identity')
            if n == 1:
                self.ready = dict(sample)
            else:
                require(self.ack_ns <= sample['start_ns']
                        and sample['raw_bytes'] > self.ready['raw_bytes'], 'workload before verified ack')
        else:
            require(sample is None, 'unexpected event sample')
        if n == 2:
            require(self.ready is not None and self.ready['end_ns'] <= stamp, 'release before persisted readiness')
        if n == 3:
            self.ack_ns = stamp
        self.sequence += 1
        self.last_ns = stamp
        return message

    def message(self, event, target, *, stamp=None, sample=None):
        require(event in ORDER, 'unknown event')
        n = ORDER.index(event)
        return dict(schema=1, contract=CONTRACT, run_id=self.run_id, sequence=n,
                    event=event, owner=target, monotonic_ns=self.clock() if stamp is None else stamp,
                    state=STATES[n], sample=sample)


class Link:
    """One peer-owned connection, bounded frames/deadline/transcript; no buffering layer."""
    def __init__(self, conn, identity, deadline, journal, clock=time.monotonic_ns):
        self.conn, self.deadline, self.clock, self.journal = conn, deadline, clock, journal
        self.state, self.buffer, self.count = State(identity, clock), b'', 0
        self.peer_owner = None

    def _budget(self):
        seconds = (self.deadline - self.clock()) / 1e9
        require(seconds > 0, 'channel timeout')
        self.conn.settimeout(seconds)

    def frame(self, value):
        data = (json.dumps(value, sort_keys=True, allow_nan=False) + '\n').encode()
        require(len(data) <= MAX_MESSAGE, 'channel frame bound')
        self._budget()
        try:
            self.conn.sendall(data)
        except TimeoutError as exc:
            raise InterfaceError('channel timeout') from exc
        except OSError as exc:
            raise InterfaceError('channel closed') from exc

    def read(self):
        while b'\n' not in self.buffer:
            self._budget()
            try:
                block = self.conn.recv(MAX_MESSAGE + 1 - len(self.buffer))
            except TimeoutError as exc:
                raise InterfaceError('channel timeout') from exc
            except OSError as exc:
                raise InterfaceError('channel closed') from exc
            require(block, 'channel closed')
            self.buffer += block
            end = self.buffer.find(b'\n')
            require((end + 1 if end >= 0 else len(self.buffer)) <= MAX_MESSAGE, 'channel frame bound')
        raw, self.buffer = self.buffer.split(b'\n', 1)
        require(len(raw) + 1 <= MAX_MESSAGE, 'channel frame bound')
        try:
            return b.decode(raw)
        except ValueError as exc:
            raise InterfaceError('invalid channel JSON') from exc

    def record(self, value):
        require(self.count < 6, 'channel transcript bound')
        data = json.dumps(value, sort_keys=True, allow_nan=False) + '\n'
        require(len(data.encode()) <= MAX_MESSAGE, 'channel transcript frame bound')
        require(self.journal.write(data) == len(data), 'channel transcript short write')
        self.journal.flush()
        self.count += 1

    def send(self, event, target, *, stamp=None, sample=None):
        value = self.state.message(event, target, stamp=stamp, sample=sample)
        self.state.accept(value)
        self.frame(value)
        self.record(value)
        return value

    def receive(self, expected):
        value = self.read()
        self.state.accept(value)
        require(value['event'] == expected, 'unexpected channel event')
        self.record(value)
        return value

    def fail(self, reason):
        self.state.failed = True
        self.record(dict(schema=1, contract=CONTRACT, run_id=self.state.run_id,
                         sequence=self.state.sequence, event='error', state='failed',
                         monotonic_ns=self.clock(), error=reason))

    def close(self):
        self.conn.close()


def peer(conn, expected):
    pid, uid, _ = struct.unpack('3i', conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    require(pid == expected['pid'] and uid == os.getuid()
            and ps.same(expected, ps.identity(pid)), 'channel peer identity')


class Server:
    """Supervisor owns this private directory/listener, never a process group."""
    def __init__(self, m):
        self.m, self.listener, self.directory = m, None, None

    def __enter__(self):
        directory = Path(self.m['paths']['channel']).parent
        directory.mkdir(mode=0o700, exist_ok=False)
        self.directory = directory
        self.stat = directory.stat()
        try:
            self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.listener.bind(self.m['paths']['channel'])
            self.listener.listen(1)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def accept(self, runner, deadline, journal):
        seconds = (deadline - time.monotonic_ns()) / 1e9
        require(seconds > 0, 'channel timeout')
        self.listener.settimeout(seconds)
        try:
            conn, _ = self.listener.accept()
        except TimeoutError as exc:
            raise InterfaceError('channel timeout') from exc
        try:
            peer(conn, runner)
            link = Link(conn, self.m['run_id'], deadline, journal)
            link.peer_owner = identity(runner)
            hello = link.read()
            require(hello == dict(schema=1, contract=CONTRACT, kind='hello', run_id=self.m['run_id'],
                                 runner=identity(runner), supervisor_pid=os.getpid(),
                                 config_sha256=b.sha(b.read_regular(self.m['paths']['config']))), 'channel hello identity')
            link.frame(dict(hello, kind='connected'))
            return link
        except BaseException:
            conn.close()
            raise

    def __exit__(self, *args):
        if self.listener:
            self.listener.close()
        if self.directory:
            st = self.directory.stat()
            require((st.st_dev, st.st_ino, st.st_uid) ==
                    (self.stat.st_dev, self.stat.st_ino, os.getuid()), 'channel directory substituted')
            Path(self.m['paths']['channel']).unlink(missing_ok=True)
            self.directory.rmdir()


def connect(m, deadline, journal):
    supervisor = ps.identity(os.getppid())
    require(supervisor is not None, 'channel parent unavailable')
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    link = Link(conn, m['run_id'], deadline, journal)
    try:
        link._budget()
        conn.connect(m['paths']['channel'])
        peer(conn, supervisor)
        link.peer_owner = {k: supervisor[k] for k in ps.KEYS}
        runner = ps.identity(os.getpid())
        require(runner is not None and runner['pid'] == runner['pgid'] == runner['sid'], 'channel Runner session')
        hello = dict(schema=1, contract=CONTRACT, kind='hello', run_id=m['run_id'],
                     runner=identity(runner), supervisor_pid=supervisor['pid'],
                     config_sha256=b.sha(b.read_regular(m['paths']['config'])))
        link.frame(hello)
        require(link.read() == dict(hello, kind='connected'), 'channel confirmation identity')
        return link
    except BaseException:
        conn.close()
        raise


def receipt(recorder, row):
    return dict(sequence=row['sequence'], start_ns=row['start_ns'], end_ns=row['end_ns'],
                raw_bytes=recorder.bytes, raw_sha256=recorder.hash.hexdigest())


def verify_receipt(m, message, phase):
    """Check that readiness really reached the shared file before releasing Go."""
    sample = message['sample']
    # Further monitor rows may already have been appended. Bind the acknowledged
    # prefix, not a later length, and never permit replacement or prefix mutation.
    fd = os.open(m['paths']['raw'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        st = os.fstat(fd)
        require(stat.S_ISREG(st.st_mode) and st.st_nlink == 1
                and sample['raw_bytes'] <= st.st_size <= 16 * 1024 * 1024, 'persisted RSS file shape')
        with os.fdopen(os.dup(fd), 'rb') as stream:
            raw = stream.read(sample['raw_bytes'])
            stream.seek(0)
            require(stream.read(sample['raw_bytes']) == raw, 'persisted RSS prefix changed')
        current = os.stat(m['paths']['raw'], follow_symlinks=False)
        require((st.st_dev, st.st_ino) == (current.st_dev, current.st_ino), 'persisted RSS file changed')
    finally:
        os.close(fd)
    require(len(raw) == sample['raw_bytes'] and hashlib.sha256(raw).hexdigest() == sample['raw_sha256'],
            'persisted RSS receipt identity')
    rows = [b.decode(x) for x in raw.splitlines()]
    require(len(rows) == sample['sequence'] + 1 and rows, 'persisted RSS sequence')
    row = rows[-1]
    require(row['run_id'] == m['run_id'] and row['owner'] == message['owner']
            and row['sequence'] == sample['sequence'] and row['start_ns'] == sample['start_ns']
            and row['end_ns'] == sample['end_ns'] and row['phase']['name'] == phase
            and row['sample_status'] == 'COMPLETE', 'persisted RSS receipt disagreement')
