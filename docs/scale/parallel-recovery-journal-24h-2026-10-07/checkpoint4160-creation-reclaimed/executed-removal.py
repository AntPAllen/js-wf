from storage_review_common import *
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07'
out=base/'checkpoint4160-creation-reclaimed';out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
def blob(p):
 b=subprocess.check_output(['git','cat-file','blob',head+':'+str(p.relative_to(repo))],cwd=repo);assert b==p.read_bytes();return b
report=dict(head=head,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_before=shutil.disk_usage('/tmp').free,removed=[],scope='Closed initial failed creation fixture with nested initial live-container archive, cached-metadata component and exact-profile accepted creation trial, archives and redundant original download only. Full native verdicts retained. Active campaigns and inputs preserved.')
def save():
 report['free_after']=shutil.disk_usage('/tmp').free;(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def remote(url,expected):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
  p.stdin.write(s3.credentials());p.stdin.close()
  try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
  except BaseException:p.kill();p.wait();raise
  err=p.stderr.read();assert p.wait()==0,err
 return declared,actual
items=[('checkpoint4160-creation-initial-fixture-failure',Path('/tmp/js-wf-checkpoint4160-creation-stall-20261007'),Path('/tmp/js-wf-checkpoint4160-creation-stall-closed-20261007.tar.gz'),'js-wf-checkpoint4160-creation-stall-20261007.service','433dc0e0b313488899a825edf8bc48d7',2555639,2,'exit-code'),('checkpoint4160-creation-cached-metadata-component',Path('/tmp/js-wf-checkpoint4160-creation-stall-v2-20261007'),Path('/tmp/js-wf-checkpoint4160-creation-stall-v2-20261007.tar.gz'),'js-wf-checkpoint4160-creation-stall-v2-20261007.service','00ae259139874281b23b7f0aa7a0b722',2594988,0,'success'),('checkpoint4160-creation-exact-profile-accepted',Path('/tmp/js-wf-checkpoint4160-creation-stall-v3-20261007'),Path('/tmp/js-wf-checkpoint4160-creation-stall-v3-20261007.tar.gz'),'js-wf-checkpoint4160-creation-stall-v3-20261007.service','9e67bb9ee6af42ad972f2d1fce9e4b36',2604420,0,'success')]
worktrees=[Path(x[9:]) for x in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if x.startswith('worktree ')]
for name,root,archive,unit,invocation,pid,code,result in items:
 proof=base/name;meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'));invbytes=blob(proof/'fixture-inventory.json');inventory=json.loads(invbytes)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
 raw=subprocess.check_output(['systemctl','--user','show',unit,'-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'],text=True)
 f=dict(line.split('=',1) for line in raw.splitlines());assert f==dict(MainPID='0',ExecMainPID=str(pid),ExecMainStatus=str(code),Result=result,InvocationID=invocation)
 assert not any(w.exists() and (w.is_relative_to(root) or root.is_relative_to(w)) for w in worktrees)
 record=dict(root=str(root),canonical=str(proof.relative_to(repo)),original_unit=raw,closure_before=closure(root))
 assert fixture_archive.inventory(root)==inventory['files'];record['current_inventory_matches']=True
 if code==2:
  obsolete=Path(str(root)+'-proof')
  assert fixture_archive.inventory(obsolete)==fixture_archive.inventory(root/'initial-live-container-archive-proof')
  record['obsolete_producer_metadata_closure']=closure(obsolete)
  record['obsolete_producer_metadata_matches_nested_preserved_copy']=True
  extra_allocated=sum(x.stat().st_blocks*512 for x in [obsolete,*obsolete.rglob('*')] if x.stat().st_nlink==1 or x.is_dir())
 else: extra_allocated=0
 print('VERIFY_REMOTE',name,flush=True)
 declared,actual=remote(receipt['archive']['url'],expected);assert declared==inventory
 record.update(remote_full_readback=actual,remote_all_members_match=True)
 inodes={};dirs=0
 for p in [root,*root.rglob('*')]:
  st=p.stat()
  if p.is_dir():dirs+=st.st_blocks*512;continue
  k=(st.st_dev,st.st_ino)
  if k not in inodes:inodes[k]=[st,0]
  inodes[k][1]+=1
 allocated=dirs+sum(st.st_blocks*512 for st,n in inodes.values() if n==st.st_nlink)
 record['closure_immediately_before_removal']=closure(root);report['verified_pending_removal']=record;save()
 shutil.rmtree(root);assert not root.exists()
 if code==2:shutil.rmtree(obsolete);assert not obsolete.exists()
 allocated+=extra_allocated
 assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
 with archive.open('rb') as f:assert s3.digest(f)==expected
 record['archive_closure']=closure(archive);allocated+=archive.stat().st_blocks*512;archive.unlink()
 record.update(removed=True,released_allocated_bytes=allocated);report['removed'].append(record);report.pop('verified_pending_removal',None);save()
 print('REMOVED',name,allocated,flush=True)
archive=Path('/tmp/js-wf-parallel-recovery-journal-24h-restoration-20261007.tar.gz');proof=base/'terminal-failure';meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'));inventory=json.loads(blob(proof/'fixture-inventory.json'))
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
with archive.open('rb') as f:assert s3.digest(f)==expected
print('VERIFY_REMOTE_ORIGINAL_DOWNLOAD',flush=True)
declared,actual=remote(receipt['archive']['url'],expected);assert declared==inventory
record=dict(archive=str(archive),remote_full_readback=actual,remote_all_members_match=True,closure_before_removal=closure(archive),released_allocated_bytes=archive.stat().st_blocks*512)
report['verified_pending_removal']=record;save();archive.unlink();record['removed']=True;report['removed'].append(record);report.pop('verified_pending_removal',None)
report['released_allocated_bytes']=sum(r['released_allocated_bytes'] for r in report['removed']);report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['released_allocated_bytes'],flush=True)
