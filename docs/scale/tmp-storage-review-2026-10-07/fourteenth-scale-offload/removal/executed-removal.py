from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py')
s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(path):
    data = subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo)
    assert (repo/path).read_bytes() == data
    return data
def remote(url, expected):
    with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3',
        '--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
        p.stdin.write(config);p.stdin.close()
        try: result=fixture_archive.verify_hashed_stream(p.stdout,expected)
        except BaseException: p.kill();p.wait();raise
        error=p.stderr.read();assert p.wait()==0,error
    return result
config=s3.credentials()
out=Path('/tmp/storage-scale-offload-removal-20261007');out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py')
shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report=dict(head=head,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_before=shutil.disk_usage('/tmp').free,removed=[],scope='Two closed 400k donor store directories moved to verified S3. Source worktrees and local producer/review records remain. Completed native blob boundary root and archive retired. Live24h and million fixtures retained. Original test verdicts unchanged.')
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
items=[
 ('js-wf-bulk-latency-full400k-valid-20261006','docs/scale/full400k-bulk-capacity-preparation-2026-10-06/native-valid-population','fixture'),
 ('js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005','docs/scale/tmp-storage-review-2026-10-07/fourteenth-scale-offload/closed-capacity-donor','originals'),
 ('js-wf-online-blob-native-20261007','docs/scale/online-blob-boundary-2026-10-07/native-race','.')]
worktrees=[Path(line.removeprefix('worktree ')) for line in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if line.startswith('worktree ')]
save()
for name,proofname,selected in items:
 root=Path('/tmp')/name;proof=Path(proofname);target=root if selected=='.' else root/selected
 assert not any(w.is_relative_to(target) or target.is_relative_to(w) for w in worktrees),'registered worktree affected'
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'))
 manifest_bytes=blob(proof/meta['inventory_file']);inventory=json.loads(manifest_bytes)
 assert hashlib.sha256(manifest_bytes).hexdigest()==meta['inventory_sha256']
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 before=closure(root)
 assert fixture_archive.inventory(root)==inventory['files'],'current full inventory mismatch'
 print('VERIFY_REMOTE',name,flush=True)
 declared,actual=remote(receipt['archive']['url'],expected);assert declared==inventory
 assert fixture_archive.inventory(root)==inventory['files'],'current full inventory changed'
 after=closure(root)
 record=dict(root=str(root),removed_store=str(target),canonical_metadata=str(proof/'archive-verification.json'),receipt=str(proof/'s3-readback.json'),archive_url=receipt['archive']['url'],full_remote_archive_and_every_member_verified=actual,files=len(inventory['files']),local_complete_tree_matches_committed_inventory=True,closure_before=before,closure_immediately_before_removal=after,removed_allocated_bytes=0,source_worktree_preserved=selected!='.')
 allocated=sum(p.stat().st_blocks*512 for p in target.rglob('*'))+target.stat().st_blocks*512
 report['pending_verified_removal']=record;save()
 shutil.rmtree(target);assert not target.exists();record['removed_allocated_bytes']+=allocated
 archive=Path(str(root)+'.tar.gz')
 if archive.exists():
  assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
  with archive.open('rb') as f:assert s3.digest(f)==expected
  record['archive_closure']=closure(archive);allocated=archive.stat().st_blocks*512
  report['pending_verified_removal']=record;save()
  archive.unlink();record['removed_allocated_bytes']+=allocated;record['removed_archive']=str(archive)
 report.pop('pending_verified_removal',None);report['removed'].append(record);save()
 print('REMOVED',name,record['removed_allocated_bytes'],flush=True)
report['removed_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['removed_allocated_bytes'],report['free_bytes_after'],flush=True)
