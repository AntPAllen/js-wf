from pathlib import Path
import json,hashlib,subprocess,os,time
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/callback-audit-delivery-2026-10-05/headroom-capacity';out.mkdir(parents=True,exist_ok=True)
dirs=['docs/scale/compact-audit-metadata-2026-10-05/four-core-400k-comparison', 'docs/scale/callback-audit-delivery-2026-10-05/legacy-controls', 'docs/scale/tier2-retained-row-2026-10-05/copied-smoke-v1', 'docs/scale/result-absence-2026-10-05/race', 'docs/scale/continuation-retirement-domain-2026-10-05', 'docs/scale/continuation-domain-all-server-restart-2026-10-05/qualified', 'docs/scale/continuation-domain-all-server-restart-2026-10-05/failed-retried-heal-observation', 'docs/scale/continuation-domain-all-server-restart-2026-10-05/failed-five-second-meta-gate', 'docs/scale/continuation-domain-all-server-restart-2026-10-05/failed-single-heal-request', 'docs/scale/continuation-domain-all-server-restart-2026-10-05/failed-post-heal-observation', 'docs/scale/delayed-expiry-worker-kills-2026-10-05', 'docs/scale/audit-delivery-cost-2026-10-05/race-control', 'docs/scale/retained-block-media-2026-10-05/real-control', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/create/interior', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/create/first', 'docs/scale/fanout-combined-boundaries-2026-10-05/failed-drain-queue-trace', 'docs/scale/snapshot-manifest-leader-2026-10-05/corrected', 'docs/scale/snapshot-manifest-leader-2026-10-05/baseline', 'docs/scale/result-absence-2026-10-05/corrected', 'docs/scale/result-absence-2026-10-05/baseline', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/create/first', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-diagnosis/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/create/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/results/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/create/interior', 'docs/scale/fanout-combined-boundaries-2026-10-05/failed-drain-queue-diagnosis', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/create/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/results/interior', 'docs/scale/tier2-retained-profiles-2026-10-05/legacy-control', 'docs/scale/callback-audit-delivery-2026-10-05/native-fault-controls', 'docs/scale/fanout-combined-boundaries-2026-10-05/drained-copied-audits/results/first', 'docs/scale/fanout-combined-boundaries-2026-10-05/accepted-queue-diagnosis', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/results/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/confirmed-copied-diagnosis/last', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/results/first', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-diagnosis/interior', 'docs/scale/fanout-combined-boundaries-2026-10-05/copied-audits/results/interior']
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert head==remote
records=[]
for d in dirs:
 manifest=json.loads((repo/d/'archive-verification.json').read_text())
 for part in manifest['parts']:
  name=d+'/'+part['file'];p=repo/name;data=p.read_bytes();assert hashlib.sha256(data).hexdigest()==part['sha256'] and len(data)==part['bytes']
  canonical=subprocess.check_output(['git','cat-file','blob',head+':'+name],cwd=repo);assert canonical==data
  blob=subprocess.check_output(['git','rev-parse',head+':'+name],cwd=repo,text=True).strip();assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==blob
  records.append({'path':name,'bytes':len(data),'allocated_bytes':p.stat().st_blocks*512,'sha256':part['sha256'],'git_blob':blob})
paths={str(repo/r['path']) for r in records};fds=[]
for proc in Path('/proc').glob('[0-9]*'):
 try:
  for fd in (proc/'fd').iterdir():
   try:
    if os.readlink(fd) in paths:fds.append(str(fd))
   except (OSError,PermissionError):pass
 except (OSError,PermissionError):pass
assert not fds,fds
patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before)
extra=''.join('!/'+r['path']+'\n' for r in records);patterns.write_text(before+extra)
subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for r in records:assert not (repo/r['path']).exists();assert subprocess.check_output(['git','rev-parse',head+':'+r['path']],cwd=repo,text=True).strip()==r['git_blob']
(out/'recovery.json').write_text(json.dumps({'source':head,'pushed_main_matches':True,'all_copies_match_manifest_and_canonical_git':True,'no_visible_descriptors':True,'allocated_bytes_recovered':sum(r['allocated_bytes'] for r in records),'scope':'Only verified pushed worktree archive parts; original failed/live fixtures and original canonical archives retained','parts':records},indent=2)+'\n');(out/'executed-recovery.py').write_bytes(Path(__file__).read_bytes());print('RECOVERED',sum(r['allocated_bytes'] for r in records),flush=True)
