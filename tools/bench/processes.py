"""Linux identity/RSS observation. No name matching and no signaling by stale PGID."""
import hashlib,os,signal
from pathlib import Path

KEYS=('pid','start_ticks','pgid','sid')
FILE_HASHES={}
def identity(pid):
 try:
  raw=Path(f'/proc/{pid}/stat').read_text();x=raw[raw.rfind(')')+2:].split()
  return dict(pid=pid,state=x[0],ppid=int(x[1]),pgid=int(x[2]),sid=int(x[3]),start_ticks=int(x[19]))
 except (FileNotFoundError,ProcessLookupError):return None

def same(a,b):return a is not None and b is not None and all(a[k]==b[k] for k in KEYS)

def executable(pid):
 p=Path(f'/proc/{pid}/exe');st=p.stat()
 key=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)
 if key not in FILE_HASHES:FILE_HASHES[key]=hashlib.sha256(p.read_bytes()).hexdigest()
 return dict(path=os.readlink(p),device=st.st_dev,inode=st.st_ino,sha256=FILE_HASHES[key])

def observe(owner):
 start=__import__('time').monotonic_ns();a=identity(owner['pid'])
 if a is None:raise ProcessLookupError('target_gone')
 if not same(a,owner):raise RuntimeError('target_identity_changed')
 if a['state'] in ('Z','X'):return dict(valid=True,live=False,identity=a,start_ns=start,end_ns=__import__('time').monotonic_ns(),rss_bytes=None,missing='not_live')
 exe=executable(a['pid']);rss=int(Path(f"/proc/{a['pid']}/statm").read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
 b=identity(a['pid'])
 if not same(a,b) or b['state'] in ('Z','X'):raise ProcessLookupError('target_exited_during_sample')
 if executable(a['pid'])!=exe:raise RuntimeError('target_executable_changed')
 available=next(int(x.split()[1])*1024 for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:'))
 return dict(valid=True,live=True,identity=b,executable=exe,rss_bytes=rss,system_available_bytes=available,start_ns=start,end_ns=__import__('time').monotonic_ns())

def members(owner):
 if owner is None:return []
 result=[]
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  i=identity(int(p.name))
  if i and i['sid']==owner['sid'] and i['state'] not in ('Z','X'):result.append(i)
 return result

def send(owner,number):
 if not owner or not same(owner,identity(owner['pid'])):raise RuntimeError('unverifiable_session_anchor')
 if owner['pid']<=1 or owner['pgid']!=owner['pid'] or owner['sid']!=owner['pid'] or owner['pgid']==os.getpgrp():raise RuntimeError('unsafe_session_anchor')
 try:os.killpg(owner['pgid'],number)
 except ProcessLookupError:pass
