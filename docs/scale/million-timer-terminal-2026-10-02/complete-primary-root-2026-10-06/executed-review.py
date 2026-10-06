from pathlib import Path
import sys,json,subprocess,hashlib,tarfile,io,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
root=Path('/tmp/js-wf-timer-volume-million-service-20261001');raw=Path('/tmp/js-wf-million-primary-complete-20261006.tar.gz');proof=Path('/tmp/js-wf-million-primary-complete-20261006-proof')
meta=json.loads((proof/'archive-verification.json').read_text());manifest=json.loads((proof/'fixture-inventory.json').read_text())
assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
with raw.open('rb') as stream:assert fixture_archive.digest(stream)==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
base='docs/scale/million-timer-terminal-2026-10-02/'
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+base+name],cwd=repo)
old_manifest={x['path']:x for x in json.loads(blob('store-copy/manifest.json'))}
old_archive=blob('store-copy/originals.tar.gz')
actual={};original_files=None
with tarfile.open(fileobj=io.BytesIO(old_archive),mode='r:gz') as archive:
 for member in archive:
  assert member.isfile() and member.name in old_manifest and member.name not in actual
  data=archive.extractfile(member).read();entry=old_manifest[member.name]
  assert len(data)==entry['bytes'] and hashlib.sha256(data).hexdigest()==entry['sha256']
  actual[member.name]=True
  if member.name=='original-store-before.json':original_files=json.loads(data)
assert set(actual)==set(old_manifest) and original_files is not None
expected={x['path']:x['sha256'] for x in original_files}
current={name.removeprefix('cluster/'):entry['sha256'] for name,entry in manifest['files'].items() if name.startswith('cluster/')}
assert expected==current and len(current)==4995
terminal={x['path']:x for x in json.loads(blob('manifest.json'))}
for original,prior in [('receipts.bin','receipts.bin'),('observations.bin','receipt-recovery/observations.bin'),('report.json','report.json')]:
 assert manifest['files'][original]['sha256']==terminal[prior]['sha256'] and manifest['files'][original]['bytes']==terminal[prior]['bytes']
report=json.loads((root/'report.json').read_text());assert report['status']=='failed' and report['unique_received']==1000000 and report['observations_sha256']==manifest['files']['observations.bin']['sha256']
out=repo/'docs/scale/million-timer-terminal-2026-10-02/complete-primary-root-2026-10-06';out.mkdir()
for name in ['capture.json','executed-capture.py','archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/name,out/name)
shutil.copyfile(__file__,out/'executed-review.py')
review=dict(current_git_revision=head,original_run_revision=report['revision'],source_root=str(root),complete_archive=meta,all_4995_current_cluster_files_match_original_archived_preinspection_ledger=True,previous_store_copy_archive_all_members_verified=True,report_receipts_and_observations_match_committed_terminal_manifest=True,original_report_status=report['status'],scope='Complete original million-service storage custody only. Physical stores, executable, durable ledgers and report preserved without reopening. Historical failed native drain verdict unchanged; report zeros are defaults, not measurements. Further native store diagnosis must use a fresh verified restoration.')
(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(dict(members=meta['members'],bytes=meta['archive_bytes'],unchanged_store_files=len(current))))
