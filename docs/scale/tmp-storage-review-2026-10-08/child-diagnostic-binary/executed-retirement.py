import sys,json,subprocess,hashlib,shutil,datetime,os
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
base=repo/'docs/scale/tmp-storage-review-2026-10-08/child-diagnostic-binary'
if len(sys.argv)>1:
    library=repo/'docs/scale/tmp-storage-review-2026-10-08/followup-small-files/executed-closure-library.py'
    ns={'__name__':'closure_library'};exec(compile(library.read_text(),str(library),'exec'),ns)
    c=json.loads((base/'capture.json').read_text())
    print(json.dumps(ns['closure']([Path(p) for p in c['roots']+[c['stage'],c['archive']]])))
    sys.exit()
import boto3
for name in ['archive-verification.json','fixture-inventory.json','s3-readback.json','capture.json']:
    p=base/name
    assert subprocess.check_output(['git','show','HEAD:'+str(p.relative_to(repo))],cwd=repo)==p.read_bytes()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo)==subprocess.check_output(['git','rev-parse','origin/main'],cwd=repo)
c=json.loads((base/'capture.json').read_text());inv=json.loads((base/'fixture-inventory.json').read_text());meta=json.loads((base/'archive-verification.json').read_text());receipt=json.loads((base/'s3-readback.json').read_text())
s3=boto3.client('s3',endpoint_url=receipt['endpoint'],aws_access_key_id='x',aws_secret_access_key='x')
response=s3.get_object(Bucket=receipt['bucket'],Key=receipt['archive_key'])
try:
    declared,remote=fixture_archive.verify_hashed_stream(response['Body'],{'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']})
finally:response['Body'].close()
assert declared==inv
stage=Path(c['stage']);archive=Path(c['archive']);roots=[Path(p) for p in c['roots']]
assert fixture_archive.inventory(stage)==inv['files']
for p in roots:
    st=p.stat();assert dict(bytes=st.st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),mode=st.st_mode&0o777,mtime_ns=st.st_mtime_ns)==inv['files'][p.name]
assert hashlib.sha256(archive.read_bytes()).hexdigest()==meta['archive_sha256']
paths=[*roots,archive,*[p for p in stage.rglob('*') if p.is_file()]]
inodes={};details=[]
for p in paths:
    st=p.stat();assert st.st_nlink==1
    inodes[st.st_dev,st.st_ino]=st.st_blocks*512
    details.append(dict(path=str(p),bytes=st.st_size,allocated_bytes=st.st_blocks*512,nlink=st.st_nlink,mtime_ns=st.st_mtime_ns))
closure=json.loads(subprocess.check_output(['sudo','-n','python3',__file__,'closure'],text=True))
assert not closure['blocked'] and not closure['permission_limits'],closure
before=shutil.disk_usage('/tmp')
for p in roots:p.unlink()
archive.unlink();shutil.rmtree(stage)
after=shutil.disk_usage('/tmp')
report=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),remote_full_body_and_members_verified=remote,remote_inventory_matches_committed=True,fresh_privileged_closure=closure,removed_files=details,distinct_inode_allocated_bytes=sum(inodes.values()),free_bytes_before=before.free,free_bytes_after=after.free,measured_free_change=after.free-before.free,measurement_limit='Concurrent verification/build activity can affect filesystem free bytes.',all_selected_paths_absent=all(not p.exists() for p in [*roots,archive,stage]))
assert report['all_selected_paths_absent']
(base/'retirement.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-retirement.py')
(base/'README.md').write_text('# Closed child-model diagnostic binary offload\n\nThe development diagnostic binary, profile, stdout and source binding are preserved in a fully verified S3 archive. Git retains the complete inventory, hashes and readback receipts. After a fresh full remote archive/member verification, unchanged-file checks and privileged process/container/mount checks, the local originals, staging copy and archive were removed.\n\nFreed '+str(sum(inodes.values()))+' allocated bytes across distinct file inodes; concurrent builds can affect the measured filesystem free-space change. This storage operation does not change any test verdict. See retirement.json for exact paths and checks.\n')
print(json.dumps({k:report[k] for k in ['distinct_inode_allocated_bytes','measured_free_change','all_selected_paths_absent']}))
