from pathlib import Path
import json,hashlib,subprocess,os,time
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/direct-callback-audit-2026-10-05/verified-r5-worktree-headroom';out.mkdir(parents=True,exist_ok=True)
dirs=['docs/scale/direct-callback-audit-2026-10-05/r5-process-owner-loss', 'docs/scale/direct-callback-audit-2026-10-05/r5-replay-diagnostic', 'docs/scale/direct-callback-audit-2026-10-05/r5-independent-cleanup', 'docs/scale/direct-callback-audit-2026-10-05/r5-position-loss-recovery', 'docs/scale/direct-callback-audit-2026-10-05/single-replica-native-fault-controls', 'docs/scale/direct-callback-audit-2026-10-05/single-replica-shutdown-recovery', 'docs/scale/direct-callback-audit-2026-10-05/capacity-400k', 'docs/scale/direct-callback-audit-2026-10-05/gc-headroom-capacity']
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
 try:tasks=list((proc/'task').iterdir())
 except (OSError,PermissionError):continue
 for task in tasks:
  try:
   for fd in (task/'fd').iterdir():
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
