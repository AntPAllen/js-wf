from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)
import tarfile
from fixture_delta import safe_name
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
root=Path('/tmp')/'js-wf-tier3-clock131-143-37164231641'
canonical='docs/scale/current-tier3-clock-2026-10-04/worker-clock-131-143/summary.json'
summary=json.loads(subprocess.check_output(['git','show',head+':'+canonical],cwd=repo))
archive=root/'original-stores/rolling-originals.tar.gz';manifest=root/'original-stores/rolling-manifest.json'
expected=dict(bytes=summary['canonical_archive_bytes'],sha256=summary['canonical_archive_sha256'])
assert str(archive)==summary['canonical_archive_path']
initial=closure(root);before=fixture_archive.inventory(root)
with archive.open('rb') as stream:assert s3.digest(stream)==expected
wanted=json.loads(manifest.read_text())['files'];actual={}
with tarfile.open(archive,'r|gz') as tar:
 for member in tar:
  name=safe_name(member.name);assert member.isfile() and name not in actual
  record=s3.digest(tar.extractfile(member));assert record['bytes']==member.size
  record.update(mode=member.mode,mtime=member.mtime);actual[name]=record
assert {k:v['sha256'] for k,v in actual.items()}==wanted
assert len(actual)==summary['canonical_archive_members']==51519
with archive.open('rb') as stream:assert s3.digest(stream)==expected
assert fixture_archive.inventory(root)==before
final=closure(root)
out=repo/'docs/scale/tmp-storage-review-2026-10-06/worker-clock-131-143';out.mkdir(parents=True)
(out/'member-inventory.json').write_text(json.dumps(actual,indent=2)+'\n')
(out/'root-inventory.json').write_text(json.dumps(before,indent=2)+'\n')
shutil.copyfile(manifest,out/'rolling-manifest.json');shutil.copyfile(__file__,out/'executed-review.py')
meta=dict(schema='js-wf-legacy-rolling-archive-v1',archive_bytes=expected['bytes'],archive_sha256=expected['sha256'],archive_members=len(actual),archive_parts=[],archive_parts_count=0,all_archive_members_read_back=True,all_archive_members_and_parts_read_back=True,member_inventory_sha256=hashlib.sha256((out/'member-inventory.json').read_bytes()).hexdigest(),legacy_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),root_inventory_sha256=hashlib.sha256((out/'root-inventory.json').read_bytes()).hexdigest(),canonical_summary=canonical,scope='Existing canonical full rolling archive exclusively local, with zero Git archive parts. Full compressed bytes equal committed summary; every regular safe member hash equals original rolling manifest. This flag covers all members and zero parts; no fictitious Git part verification. No stored fixture reopened or new native qualification.')
(out/'archive-verification.json').write_text(json.dumps(meta,indent=2)+'\n')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),head=head,root=str(root),archive=str(archive),canonical_summary=canonical,archive_equals_committed_hash_size=True,all_regular_members_match_legacy_manifest=len(actual),current_root_files_unchanged=len(before),closure_before=initial,closure_after=final,scope='Preservation of exact existing accepted recorded-source worker-clock seeds131-143 physical archive. Historical SDK/source/native qualification scope unchanged. No closed archive deletion until committed metadata, full S3 readback, fresh archive/member/root census/visible closure verification; other root reports/models/raw evidence retained.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print('LEGACY_CLOCK_ARCHIVE_ALL_MEMBERS_CURRENT_ROOT_CLOSURE_VERIFIED',expected,len(actual),flush=True)
