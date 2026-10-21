"""Independent evidence-only audit. Never imports the observer or invokes target code."""
import hashlib,json,os
from pathlib import Path
RUNNER_SHA='b31448c0114902b9f4888077e26f6153453c7f413bf73ad58244a31c3cd6b9ce'
KEYS=('pid','start_ticks','sid','pgid')
def same(a,b):return all(a.get(k)==b.get(k) for k in KEYS)

def audit(path,require_success=True):
 path=Path(path);r=json.loads((path/'supervisor.json').read_text());rows=[json.loads(x) for x in (path/'resources.jsonl').read_text().splitlines()]
 assert r['runner_sha256']==RUNNER_SHA
 exe=Path(r['target_argv'][0]).resolve();st=exe.stat();h=hashlib.sha256(exe.read_bytes()).hexdigest()
 expected=r['expected_executable'];assert (st.st_dev,st.st_ino,h)==(expected['device'],expected['inode'],expected['sha256'])
 owner=r.get('target_owner');live=[];last=-1
 for s in rows:
  assert s['start_ns']<=s['end_ns'] and s['start_ns']>=last;last=s['end_ns']
  if not s.get('valid') or not s.get('live'):continue
  assert owner and same(s['identity'],owner) and s['identity']['state'] not in ['Z','X'] and s['rss_bytes']>0
  assert (s['executable']['device'],s['executable']['inode'],s['executable']['sha256'])==(st.st_dev,st.st_ino,h)
  assert s['system_available_bytes']>0
  if s['classification']=='pre_release':assert r['release_begin_ns'] is None or s['end_ns']<=r['release_begin_ns']
  else:assert s['classification']=='post_release' and r['release_complete_ns']<=s['start_ns']
  live.append(s)
 pre=[s for s in live if s['classification']=='pre_release'];post=[s for s in live if s['classification']=='post_release']
 if pre:
  assert r['peer']['pid']==owner['pid'] and r['peer']['uid']==os.getuid()
 complete=bool(pre and r['ack_ns'] and r['release_begin_ns'] and pre[-1]['end_ns']<=r['release_begin_ns']<=r['release_complete_ns']<=r['ack_ns'])
 assert r['observation_complete']==complete
 assert r['pre_release_live_sample']==(pre[-1] if pre else None)
 assert r['readiness_rss_bytes']==(pre[-1]['rss_bytes'] if pre else None)
 assert r['all_live_sampled_peak_bytes']==max((s['rss_bytes'] for s in live),default=None)
 assert r['post_release_live_samples']==len(post)
 assert r['post_release_sampled_peak_bytes']==max((s['rss_bytes'] for s in post),default=None)
 assert r['actual_lifetime_peak_bytes'] is None
 assert r['post_release_coverage']==('sampled_not_complete_peak' if post else 'unobserved')
 if r['success'] or require_success:
  assert r['success'] and complete and not r['reason'] and not r['errors']
  assert r['cleanup']['success'] and r['cleanup']['direct_reaped'] and r['cleanup']['residual_live']==[]
  assert r['inner_primary']['outcome']=='command_success' and r['inner_cleanup']['success'] and r['inner_cleanup']['direct_exit_code']==0
  events=r['events'];assert all(a['monotonic_ns']<=b['monotonic_ns'] for a,b in zip(events,events[1:]))
  order=[e['event'] for e in events]
  assert order.index('target_identified')<order.index('sampler_ready')<order.index('before_release')<order.index('released')<order.index('target_acknowledged')
 return dict(resource_classification='incomplete' if not complete else ('readiness_only' if not post else 'readiness_and_sampled_post_release'),
  pre_release_live_samples=len(pre),post_release_live_samples=len(post),readiness_rss_bytes=r['readiness_rss_bytes'],post_release_sampled_peak_bytes=r['post_release_sampled_peak_bytes'],actual_lifetime_peak_bytes=None,
  primary=r.get('inner_primary'),cleanup_success=r['cleanup']['success'],overall_command_success=r['success'],pilot_acceptance='NOT_ASSESSED')

if __name__=='__main__':
 import argparse
 p=argparse.ArgumentParser();p.add_argument('path');a=p.parse_args();print(json.dumps(audit(a.path),indent=2))
