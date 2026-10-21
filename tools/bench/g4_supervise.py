"""G4-only derived Supervisor v1. Legacy callers delegate to unchanged supervise.py.

The selected G4 branch retains the reviewed Supervisor loop and legacy.finish;
only the explicit bound entrypoint/environment and protocol hooks differ.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

from . import g4_binding as binding, g4_events as events, policy_binding as b
from . import supervise as legacy
from .supervise import RUNNER, RUNNER_SHA, REPO, GATES, ps, finish, emit


class Controller:
    def __init__(self, m, server, journal, deadline):
        self.m, self.server, self.journal, self.deadline = m, server, journal, deadline
        self.link = None

    def runner_created(self, rec, proc, runner, target, conn):
        self.link = self.server.accept(runner, self.deadline, self.journal)

    def ready(self, rec, proc, runner, target, conn):
        self.link.send('ready_verified', events.identity(target), stamp=rec['events'][-1]['monotonic_ns'])

    def before_release(self, rec, proc, runner, target, conn):
        value = self.link.receive('readiness_persisted')
        events.verify_receipt(self.m, value, 'readiness')

    def released(self, rec, proc, runner, target, conn):
        self.link.send('release_initiated', events.identity(target), stamp=rec['release_begin_ns'])

    def acknowledged(self, rec, proc, runner, target, conn):
        self.link.send('release_acknowledged', events.identity(target), stamp=rec['ack_ns'])
        value = self.link.receive('workload_persisted')
        events.verify_receipt(self.m, value, 'workload')

    def complete(self):
        return self.link is not None and self.link.state.sequence == 5 and not self.link.state.failed

    def close(self):
        if self.link is not None:
            try:
                if not self.complete():
                    self.link.fail('incomplete_supervisor_coordination')
            finally:
                self.link.close()


def supervise(argv, out, *, entrypoint='legacy', config=None, **options):
    """Explicit selection; no arbitrary command, implicit fallback or G2 reinterpretation."""
    if entrypoint == 'legacy':
        binding.require(config is None, 'legacy entrypoint cannot use G4 config')
        return legacy.supervise(argv, out, **options)
    binding.require(entrypoint in (binding.ENTRYPOINT, binding.CRYPTO_ENTRYPOINT) and config is not None and not options,
                    'unapproved Supervisor entrypoint/options')
    m = binding.check(config, live=True)
    binding.require(entrypoint == m['entrypoint'] and argv == m['target_argv'] and str(Path(out).absolute()) == m['paths']['supervised'],
                    'G4 Supervisor launch mismatch')
    p = m['effective_limits']
    with events.Server(m) as server, (Path(m['paths']['out']) / 'supervisor-events.jsonl').open('x') as journal:
        controller = Controller(m, server, journal, time.monotonic_ns() + int(p['barrier_seconds'] * 1e9))
        try:
            return _supervise(argv, out, launch=m, controller=controller,
                    env_overrides=binding.launch_overrides(m), role=binding.launch_role(m),
                    observer=ps.observe,
                    barrier_seconds=p['barrier_seconds'], outer_seconds=p['outer_seconds'],
                    inner_seconds=p['wall_seconds'], hooks=dict(runner_created=controller.runner_created,
                    target_identified=controller.ready, before_release=controller.before_release,
                    released=controller.released, target_acknowledged=controller.acknowledged))
        finally:
            controller.close()


def _supervise(argv,out,*,launch,controller,env_overrides=None,role='probe',barrier_seconds=2,outer_seconds=230,inner_seconds=210,observer=ps.observe,hooks=None):
 """hooks are non-CLI deterministic test injection, never experimental workload switches."""
 if any(not math.isfinite(x) or x<=0 for x in [barrier_seconds,outer_seconds,inner_seconds]):raise ValueError('positive finite budgets required')
 if hashlib.sha256(RUNNER.read_bytes()).hexdigest()!=RUNNER_SHA:raise RuntimeError('runner_hash_mismatch')
 out=Path(out);out.mkdir(parents=True,mode=0o700,exist_ok=False);hooks=hooks or {}
 expected=Path(argv[0]).resolve(strict=True);st=expected.stat()
 expected_exe=dict(path=str(expected),device=st.st_dev,inode=st.st_ino,sha256=hashlib.sha256(expected.read_bytes()).hexdigest())
 token=secrets.token_hex(32);start=time.monotonic_ns();barrier_deadline=start+int(barrier_seconds*1e9)
 rec=dict(schema=2,target_argv=argv,expected_executable=expected_exe,runner_sha256=RUNNER_SHA,supervisor_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
  barrier_seconds=barrier_seconds,outer_seconds=outer_seconds,inner_seconds=inner_seconds,role=role,reason=None,errors=[],events=[],release_begin_ns=None,release_complete_ns=None,ack_ns=None,pre_release_live_sample=None,
  observation_complete=False,post_release_live_samples=0,post_release_sampled_peak_bytes=None,readiness_rss_bytes=None,actual_lifetime_peak_bytes=None,all_live_sampled_peak_bytes=None)
 pending=[];old={};proc=runner=target=conn=None;buf=b'';hello=False;done=False;peer=None;last_sample=0;phase='await_ready';socket_error=None
 def event(name,**kw):
  value=dict(event=name,monotonic_ns=time.monotonic_ns(),**kw);rec['events'].append(value)
  if name in hooks:hooks[name](rec,proc,runner,target,conn)
 def fail(reason):
  if rec['reason'] is None:rec['reason']=reason
 def get_target():
  nonlocal target
  if target is None and (out/'inner/owner.json').exists():
   try:target=json.loads((out/'inner/owner.json').read_text());rec['target_owner']=target
   except json.JSONDecodeError:pass
 def sample(stream,classification):
  nonlocal last_sample
  last_sample=time.monotonic_ns();s=observer(target)
  if s is None:
   stream.write(json.dumps(dict(valid=False,live=False,missing='sampler_not_ready',classification=classification,start_ns=last_sample,end_ns=time.monotonic_ns()))+'\n');stream.flush();return None
  s['classification']=classification
  if not ps.same(s.get('identity'),target):raise RuntimeError('sample_identity_mismatch')
  if s.get('live'):
   exe=s['executable']
   if any(exe[k]!=expected_exe[k] for k in ['device','inode','sha256']):raise RuntimeError('sample_executable_mismatch')
   if not s['valid'] or s['rss_bytes']<=0:raise RuntimeError('invalid_live_sample')
   if s['rss_bytes']>2*1024**3 or s['system_available_bytes']<2*1024**3:raise RuntimeError('resource_safety_stop')
  stream.write(json.dumps(s)+'\n');stream.flush()
  return s
 for sig in [signal.SIGINT,signal.SIGTERM]:old[sig]=signal.getsignal(sig);signal.signal(sig,lambda n,f:pending.append(n))
 try:
  with tempfile.TemporaryDirectory(prefix='dzkr-rss-',dir='/tmp') as private, socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as listener, (out/'resources.jsonl').open('x') as trace, (out/'runner.stdout').open('xb') as stdout, (out/'runner.stderr').open('xb') as stderr:
   sockpath=str(Path(private)/'s');listener.bind(sockpath);listener.listen(1);listener.setblocking(False)
   rec['ipc']=dict(transport='AF_UNIX',directory_mode='0700',socket_path=sockpath,peer_credentials=True,token_sha256=hashlib.sha256(token.encode()).hexdigest())
   env=dict(launch['environment'])
   for name in GATES:env.pop(name,None)
   env.update(env_overrides or {})
   env.update(PYTHONDONTWRITEBYTECODE='1',GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off',GOMAXPROCS='4',DZKR_MIMCCHAIN_START_BARRIER='1',DZKR_MIMCCHAIN_BARRIER_SOCKET=sockpath,DZKR_MIMCCHAIN_BARRIER_TOKEN=token)
   env.setdefault('DZKR_MIMCCHAIN_BARRIER_TIMEOUT','5s')
   rec['safe_environment']={k:v for k,v in env.items() if k in ['GOTOOLCHAIN','GOPROXY','GOSUMDB','GOMAXPROCS','DZKR_MIMCCHAIN_START_BARRIER','DZKR_MIMCCHAIN_BARRIER_TIMEOUT',*GATES]}
   command=launch['runner_argv']
   rec['runner_argv']=command
   try:
    binding.check(launch['paths']['config'],live=True)
    proc=subprocess.Popen(command,cwd=launch['cwd'],env=env,stdout=stdout,stderr=stderr,start_new_session=True);runner=ps.identity(proc.pid);rec['runner_owner']=runner
    if not runner or runner['pid']!=runner['pgid'] or runner['pid']!=runner['sid']:raise RuntimeError('runner_identity_unavailable')
    event('runner_created')
    while True:
     get_target();now=time.monotonic_ns()
     if pending:fail('supervisor_interrupted');break
     if now-start>outer_seconds*1e9:fail('outer_timeout');break
     # Consume buffered peer bytes before interpreting process exit; short targets
     # can write the acknowledgement and exit between observer iterations.
     if conn is None:
      try:
       conn,_=listener.accept();conn.setblocking(False)
       pid,uid,gid=struct.unpack('3i',conn.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12));peer=dict(pid=pid,uid=uid,gid=gid);rec['peer']=peer;event('peer_connected')
      except BlockingIOError:pass
     if conn is not None and not done:
      try:
       block=conn.recv(4096)
       if not block:raise RuntimeError('broken_ipc')
       buf+=block
       if len(buf)>4096:raise RuntimeError('oversized_handshake')
      except BlockingIOError:pass
      if b'\n' in buf and (hello or target is not None):
       line,buf=buf.split(b'\n',1);message=json.loads(line)
       if not hello:
        if target is None:raise RuntimeError('owner_not_published')
        if message.get('version')!=1 or message.get('event')!='ready' or message.get('token')!=token or message.get('role')!=role or message.get('pid')!=target['pid'] or peer['pid']!=target['pid'] or peer['uid']!=os.getuid():raise RuntimeError('readiness_identity_mismatch')
        if not ps.same(target,ps.identity(peer['pid'])):raise RuntimeError('readiness_process_changed')
        rec['target_executable']=ps.executable(peer['pid'])
        if any(rec['target_executable'][k]!=expected_exe[k] for k in ['device','inode','sha256']):raise RuntimeError('readiness_executable_mismatch')
        hello=True;phase='observe';event('target_identified')
       else:
        if rec['release_begin_ns'] is None or message!={'version':1,'event':'released','pid':target['pid'],'role':role}:raise RuntimeError('bad_release_acknowledgement')
        done=True;phase='post_release';rec['ack_ns']=time.monotonic_ns();event('target_acknowledged')
     if hello and rec['release_begin_ns'] is None:
      s=sample(trace,'pre_release')
      if s and s.get('valid') and s.get('live'):
       rec['pre_release_live_sample']=s;event('sampler_ready')
       if not ps.same(target,ps.identity(target['pid'])) or ps.executable(target['pid'])!=s['executable']:raise RuntimeError('identity_changed_before_release')
       event('before_release')
       if pending:
        fail('supervisor_interrupted');raise RuntimeError('interrupted_before_release')
       rec['release_begin_ns']=time.monotonic_ns();conn.settimeout(.2);conn.sendall(b'{"version":1,"event":"release"}\n');conn.setblocking(False);rec['release_complete_ns']=time.monotonic_ns();event('released')
     elif done and now-last_sample>=10_000_000:
      try:sample(trace,'post_release')
      except (FileNotFoundError,ProcessLookupError):
       trace.write(json.dumps(dict(valid=False,live=False,missing='exited_during_post_sample',classification='post_release',start_ns=time.monotonic_ns(),end_ns=time.monotonic_ns()))+'\n');trace.flush()
     if not done and now>=barrier_deadline:fail('barrier_timeout');break
     exited=os.waitid(os.P_PID,proc.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
     if exited is not None:
      if not done:fail('target_exited_before_handshake' if not hello else 'handshake_incomplete')
      break
     if target and not hello:
      current=ps.identity(target['pid'])
      if current is None or current['state'] in ('Z','X'):fail('target_exited_before_handshake');break
     select.select([listener] if conn is None else ([conn] if not done else []),[],[],.005)
   except BaseException as e:
    fail('monitor_or_protocol_failure');rec['errors'].append(dict(type=type(e).__name__,message=str(e)))
   finally:
    get_target()
    if conn is not None:conn.close()
    rec['cleanup']=finish(proc,runner,target)
 except BaseException as e:
  fail('supervisor_initialization_failure');rec['errors'].append(dict(type=type(e).__name__,message=str(e)))
  if 'cleanup' not in rec:rec['cleanup']=finish(proc,runner,target)
 finally:
  for sig,handler in old.items():signal.signal(sig,handler)
 try:
  inner=json.loads((out/'inner/result.json').read_text());rec['inner_result']=inner
  rec['inner_primary']=inner['primary'];rec['inner_cleanup']=inner['cleanup']
 except (OSError,ValueError,KeyError) as e:rec['inner_result']=None;rec['errors'].append(dict(stage='read_inner',type=type(e).__name__))
 samples=[json.loads(x) for x in (out/'resources.jsonl').read_text().splitlines()] if (out/'resources.jsonl').exists() else []
 live=[s for s in samples if s.get('valid') and s.get('live')]
 pre=[s for s in live if s['classification']=='pre_release'];post=[s for s in live if s['classification']=='post_release']
 rec['observation_complete']=done and bool(pre) and rec['release_begin_ns'] is not None and pre[-1]['end_ns']<=rec['release_begin_ns']
 rec['readiness_rss_bytes']=pre[-1]['rss_bytes'] if pre else None
 rec['all_live_sampled_peak_bytes']=max((s['rss_bytes'] for s in live),default=None)
 rec['post_release_live_samples']=len(post);rec['post_release_sampled_peak_bytes']=max((s['rss_bytes'] for s in post),default=None)
 rec['post_release_coverage']='sampled_not_complete_peak' if post else 'unobserved'
 inner=rec.get('inner_result') or {};cleanup=rec['cleanup']
 rec['success']=rec['reason'] is None and not rec['errors'] and rec['observation_complete'] and cleanup['success'] and inner.get('overall_success') is True and inner.get('evidence_complete') is True and not inner.get('stop_escalation') and inner.get('cleanup',{}).get('success') is True
 rec['elapsed_ns']=time.monotonic_ns()-start;rec['startup_sync_ns']=(rec['ack_ns']-start) if rec['ack_ns'] else None
 rec['pilot_acceptance']='NOT_ASSESSED_NONCRYPTO_CONTROLS_ONLY'
 rec.update(g4_contract=binding.CONTRACT,run_id=launch['run_id'],g4_config_sha256=b.sha(b.read_regular(launch['paths']['config'])),g4_interface_complete=controller.complete(),g4_channel_peer=controller.link.peer_owner if controller.link else None)
 runner_exit_code=cleanup.get('runner_exit_code')
 rec['success']=rec['success'] and rec['g4_interface_complete'] and type(runner_exit_code) is int and runner_exit_code==0
 emit(out/'supervisor.json',rec);return rec
