from pathlib import Path
import json,hashlib,subprocess,os,time
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/compact-audit-metadata-2026-10-05/headroom-four-core-proof';out.mkdir(parents=True,exist_ok=True)
dirs=['docs/scale/fanout-combined-boundaries-2026-10-05/hosted-37333174296', 'docs/scale/tier2-retained-row-2026-10-05/worker-smoke-copied-audit', 'docs/scale/tier2-retained-row-2026-10-05/workerclock-smoke-copied-audit', 'docs/scale/postgres-projection-fault-50000-2026-10-05/copied-retained-audit', 'docs/scale/fanout-combined-boundaries-2026-10-05/last-predeadline', 'docs/scale/concurrent-state-audit-2026-10-05/capacity-preparation-failed', 'docs/scale/tier2-retained-row-2026-10-05/copied-smoke-v2', 'docs/scale/terminal-state-leader-2026-10-05/corrected', 'docs/scale/byte-bounded-audit-2026-10-05/public-adoption', 'docs/scale/continuation-retirement-protobuf-before-manifest-2026-10-05', 'docs/scale/continuation-retirement-before-manifest-2026-10-05/qualified', 'docs/scale/continuation-retirement-protobuf-sigkill-2026-10-05', 'docs/scale/continuation-retirement-worker-sigkill-2026-10-05', 'docs/scale/continuation-retirement-before-manifest-2026-10-05/failed-replacement-assumption', 'docs/scale/ignored-effect-lease-loss-2026-10-05', 'docs/scale/fanout-combined-boundaries-2026-10-05/physical-drain-launch', 'docs/scale/terminal-state-leader-2026-10-05/baseline', 'docs/scale/byte-bounded-audit-2026-10-05', 'docs/scale/byte-bounded-audit-2026-10-05/native-faults', 'docs/scale/current-tier3-block-stall-2026-10-05/failed-66-78', 'docs/scale/current-tier2-matrix-2026-10-04/partition-1-12-failed', 'docs/scale/tier2-retained-row-2026-10-05/upgrade-smoke-copied-queue-diagnosis', 'docs/scale/tier2-retained-row-2026-10-05/upgrade-smoke-copied-child-diagnosis', 'docs/scale/snapshot-manifest-leader-2026-10-05/race']
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
