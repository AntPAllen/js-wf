from pathlib import Path
import json,hashlib,tarfile,os,datetime
root=Path(__file__).parent
repo=Path('/home/exedev/js-wf')
proof=repo/'docs/scale/worker-clock-checkpoint-2026-10-04/seeds112-121-diagnostics/accepted'
summary=json.loads((proof/'summary.json').read_text());compact=json.loads((proof/'manifest.json').read_text());cases=[]
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for previous in summary['cases']:
 seed=previous['seed'];p=root/f'seed{seed}';meta=json.loads((p/'artifact.json').read_text());archive=p/'originals/rolling-originals.tar.gz';manifestpath=p/'originals/rolling-manifest.json'
 assert meta['id']==previous['original_artifact'] and not meta['expired'] and meta['workflow_run']['id']==previous['run'] and meta['workflow_run']['head_sha']==summary['source']
 assert archive.stat().st_size==previous['canonical_original_archive_bytes'] and sha(archive)==previous['canonical_original_archive_sha256']
 assert sha(manifestpath)==compact['files'][f'seed-{seed}/original-stores/rolling-manifest.json']['sha256']
 expected=json.loads(manifestpath.read_text())['files'];seen=set()
 with tarfile.open(archive,'r:gz') as t:
  for m in t:
   assert m.isfile() and m.name not in seen and m.name in expected;seen.add(m.name)
   assert hashlib.file_digest(t.extractfile(m),'sha256').hexdigest()==expected[m.name],m.name
 assert seen==set(expected) and len(seen)==previous['canonical_members']==3963
 for f in [archive,manifestpath,p/'artifact.json']:
  with f.open('rb') as handle:os.fsync(handle.fileno())
 for d in [archive.parent,p,root]:
  fd=os.open(d,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
 cases.append(dict(seed=seed,run=previous['run'],source=summary['source'],artifact=meta['id'],artifact_expires_at=meta['expires_at'],canonical_path=str(archive),canonical_sha256=sha(archive),canonical_bytes=archive.stat().st_size,members_sha_verified=len(seen),manifest_matches_committed_proof=True,files_and_parent_directories_fsynced=True,local_copy_on_root_disk=True,stores_reopened=False))
result=dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),cases=cases,all_original_canonical_and_member_hashes_match_prior_accepted_proof=True,original_ram_copies_lost_after_restart=True,files_restored_to_root_disk=True,physical_archives_not_duplicated_into_git=True,qualification_unchanged=True,server_cause_confirmed=False,failed_parent_repaired=False)
with (root/'restoration.json').open('w') as f:json.dump(result,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
print(json.dumps(result,indent=2))
