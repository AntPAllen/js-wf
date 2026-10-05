from pathlib import Path
import json,hashlib,subprocess,os,sys
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/direct-callback-audit-2026-10-05/verified-manifest-proof-headroom'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
byhash={}
for name in ['docs/scale/retained-audit-phase-profile-2026-10-04/normal-100k-2g/manifest.json','docs/scale/retained-audit-streaming-2026-10-04/large-journal-fault-100k/manifest.json','docs/scale/retained-audit-streaming-2026-10-04/native-100k/manifest.json','docs/scale/continuation-promise-retained-diagnostic-2026-10-04/full-eight-cuts/manifest.json']:
 meta=json.loads((repo/name).read_text())
 assert (meta.get('all_original_member_hashes_verified') and meta.get('parts_and_concat_sha_verified')) or (meta.get('all_members_readback_sha_verified') and meta.get('concatenated_parts_readback_sha_verified'))
 byhash[meta['archive_sha256']]=(name,meta)

def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
candidates=[]
for pattern in ('js-wf-*/proof.tar.gz','js-wf-*/proof-delta.tar.gz'):
 for p in Path('/tmp').glob(pattern):
  try:e=json.loads((p.parent/'execution.json').read_text())
  except (FileNotFoundError,ValueError):continue
  if e.get('status') in ('passed','failed'):
   pid=e.get('pid')
  elif 'exit_code' in e and e.get('finished_at'):
   try:pid=json.loads((p.parent/'actual-sdk.json').read_text())['pid']
   except (FileNotFoundError,ValueError,KeyError):continue
  else:continue
  if pid is None or Path('/proc/'+str(pid)).exists():continue
  h=sha(p)
  if h in byhash:candidates.append((p,h))
paths={str(p) for p,h in candidates}
for proc in Path('/proc').glob('[0-9]*'):
 for task in (proc/'task').glob('[0-9]*'):
  try:
   for fd in (task/'fd').iterdir():
    try:assert os.readlink(fd) not in paths
    except OSError:pass
  except OSError:pass
sys.path.insert(0,str(repo/'scripts'));import fixture_delta
assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','cat-file','blob',head+':scripts/fixture_delta.py'],cwd=repo)
removed=[]
for path,digest in candidates:
 name,meta=byhash[digest]
 assert json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+name],cwd=repo))==meta
 combined=hashlib.sha256();total=0
 for part in meta['parts']:
  member=str(Path(name).parent/part.get('name',part.get('file')));data=subprocess.check_output(['git','cat-file','blob',head+':'+member],cwd=repo)
  assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256']
  combined.update(data);total+=len(data)
 assert combined.hexdigest()==digest and total==meta['archive_bytes']==path.stat().st_size
 if path.name=='proof-delta.tar.gz':assert fixture_delta.verify(path,repo)['complete_virtual_tree_verified']
 record={'temporary_duplicate':str(path),'bytes':total,'sha256':digest,'pushed_canonical_metadata':name,'base':meta.get('base')}
 path.unlink();removed.append(record)
 if sum(x['bytes'] for x in removed)>=400_000_000:break
out.mkdir(parents=True)
(out/'recovery.json').write_text(json.dumps({'pushed_revision':head,'all_visible_task_descriptors_checked':True,'scope':'Only redundant temporary combined archives from closed SDK fixtures; canonical pushed Git archive parts and required base objects retained, stores/source/media untouched','removed_bytes':sum(x['bytes'] for x in removed),'removed':removed},indent=2)+'\n')
(out/'executed-recovery.py').write_bytes(Path(__file__).read_bytes())
print('RECLAIMED',sum(x['bytes'] for x in removed),len(removed),flush=True)
