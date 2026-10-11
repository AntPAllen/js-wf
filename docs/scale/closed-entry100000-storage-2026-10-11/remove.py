"""Remove only closed byte-verified stores after committed S3 and recovery proof."""
import json,sys,hashlib,subprocess,shutil,datetime,os
from pathlib import Path
from capture import root,base,repo,closure
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
archive=Path('/home/exedev/closed-entry100000-native-20261011.tar.gz')
recovered=Path('/home/exedev/closed-entry100000-recovery-20261011')
for name in ('archive-verification.json','fixture-inventory.json','s3-readback.json','recovery.json','restored-review.json'):
    p=base/name; relative=p.relative_to(repo).as_posix()
    assert p.read_bytes()==subprocess.check_output(['git','show','HEAD:'+relative],cwd=repo)
meta=json.loads((base/'archive-verification.json').read_text())
manifest=json.loads((base/'fixture-inventory.json').read_text())
remote=json.loads((base/'s3-readback.json').read_text())
recovery=json.loads((base/'recovery.json').read_text())
review=json.loads((base/'restored-review.json').read_text())
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert remote['archive']['full_readback']==expected==recovery['archive']
assert recovery['all_bytes_modes_mtimes_verified'] and recovery['files']==3170 and recovery['restored_bytes']==3369550584
assert review['functional_limit_assertions_verified'] and not review['accepted'] and not review['actual_100000_entries_qualified']
assert review['native_files']==3170 and review['native_bytes']==3369550584 and review['fresh_restored_storage'] and review['native_storage_path']==str(recovered)
assert remote['endpoint']=='https://nameless-bird-8772.int.exe.xyz' and remote['bucket']=='nameless-bird-8772'
assert remote['metadata']['full_readback']['sha256']==hashlib.sha256((base/'archive-verification.json').read_bytes()).hexdigest()
assert remote['inventory']['full_readback']['sha256']==meta['inventory_sha256']
assert fixture_archive.verify(archive)==manifest
assert fixture_archive.inventory(root/'native')==manifest['files']==fixture_archive.inventory(recovered)
current=closure();previous=json.loads((base/'closure-after.json').read_text())
assert current['state_sha256']==previous['state_sha256'] and current['review_sha256']==previous['review_sha256']
for directory in (root,recovered):
    result=subprocess.run(['sudo','lsof','-nP','+D',str(directory)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    assert result.returncode==1 and not result.stdout and not result.stderr,(directory,result.returncode,result.stderr)
free_before=shutil.disk_usage(repo).free
rows=[]
for name,item in manifest['files'].items():
    path=root/'native'/name;st=path.stat()
    assert path.is_file() and not path.is_symlink() and st.st_size==item['bytes'] and st.st_mtime_ns==item['mtime_ns']
    rows.append(dict(path=name,bytes=st.st_size,allocated_bytes=st.st_blocks*512,links=st.st_nlink,inode=st.st_ino,device=st.st_dev))
    path.unlink()
for path in sorted((p for p in (root/'native').rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True):path.rmdir()
(root/'native').rmdir()
# These are task-created staging copies, separate from original reclaimed data.
shutil.rmtree(recovered)
archive.unlink()
report=dict(observed=datetime.datetime.now(datetime.timezone.utc).isoformat(),original_store_files_removed=len(rows),original_logical_bytes_removed=sum(r['bytes'] for r in rows),original_final_link_allocated_bytes_removed=sum(r['allocated_bytes'] for r in rows if r['links']==1),task_created_recovery_removed=True,task_created_archive_removed=True,metadata_logs_verdict_retained=True,free_before=free_before,free_after=shutil.disk_usage(repo).free,closure=current,rows=rows)
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
(root/'native-storage-archive.json').write_text(json.dumps(dict(proof_directory=str(base),remote_archive_url=remote['archive']['url'],restore_command='See proof directory README.md; restore to a fresh directory and pass --native-root to the original reviewer.',original_acceptance=False),indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ('rows','closure')},indent=2),flush=True)
