from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py')
s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(p):
 data=subprocess.check_output(['git','cat-file','blob',head+':'+str(p)],cwd=repo)
 assert (repo/p).read_bytes()==data
 return data
config=s3.credentials()
def remote(url,expected):
 with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as proc:
  proc.stdin.write(config);proc.stdin.close()
  try: result=fixture_archive.verify_hashed_stream(proc.stdout,expected)
  except BaseException:proc.kill();proc.wait();raise
  err=proc.stderr.read();assert proc.wait()==0,err
  return result
base=Path('docs/scale/tmp-storage-review-2026-10-07/eighteenth-closed-fixtures')
out=Path('/tmp/storage-state-creation-native-removal-20261007');out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py')
shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report=dict(head=head,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_before=shutil.disk_usage('/tmp').free,removed=[],scope='Initial failed and independently accepted R3 state-creation component fixtures/archives retired after fresh full S3 member/hash, local inventory and original-unit closure checks. Failed original24h and active campaigns retained. No verdict changes or store reopening.')
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
worktrees=[Path(x[9:]) for x in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if x.startswith('worktree ')]
proofbase=Path('docs/scale/parallel-recovery-journal-24h-2026-10-07')
items=[(proofbase/'watch-creation-native-initial-setup-failure',Path('/tmp/js-wf-state-watch-creation-native-20261007.tar.gz'),Path('/tmp/js-wf-state-watch-creation-native-20261007')),
       (proofbase/'watch-creation-native-accepted',Path('/tmp/js-wf-state-watch-creation-native-v2-20261007.tar.gz'),Path('/tmp/js-wf-state-watch-creation-native-v2-20261007'))]
units=[('js-wf-state-watch-creation-native-20261007.service','fd53f3201cbe4fa59cacbaa6a6cc54d6',2527446,1,'exit-code'),('js-wf-state-watch-creation-native-v2-20261007.service','9157b0f8817648aeac95603c6db93870',2528658,0,'success')]
report['original_unit_properties']=[]
for unit,invocation,pid,code,result in units:
 raw=subprocess.check_output(['systemctl','--user','show',unit,'-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'],text=True)
 fields=dict(line.split('=',1) for line in raw.splitlines())
 assert fields['MainPID']=='0' and fields['ExecMainPID']==str(pid) and fields['ExecMainStatus']==str(code) and fields['Result']==result and fields['InvocationID']==invocation
 report['original_unit_properties'].append(dict(unit=unit,properties=raw))
save()
for proof,archive,root in items:
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'))
 invbytes=blob(proof/meta['inventory_file']);inventory=json.loads(invbytes)
 assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
 with archive.open('rb') as f:assert s3.digest(f)==expected
 print('VERIFY_REMOTE',proof.name,flush=True)
 declared,actual=remote(receipt['archive']['url'],expected);assert declared==inventory
 record=dict(canonical_proof=str(proof),archive=str(archive),remote=actual,remote_all_members_match=True,released_allocated_bytes=0)
 if root:
  assert not any(w.exists() and (w.is_relative_to(root) or root.is_relative_to(w)) for w in worktrees)
  before=closure(root);assert fixture_archive.inventory(root)==inventory['files']
  # Count only inodes with all links inside the removed tree; directories separately.
  inodes={}; dirs=0
  for p in [root,*root.rglob('*')]:
   st=p.stat()
   if p.is_dir():dirs+=st.st_blocks*512;continue
   key=(st.st_dev,st.st_ino)
   if key not in inodes:inodes[key]=[st,0]
   inodes[key][1]+=1
  allocated=dirs+sum(st.st_blocks*512 for st,n in inodes.values() if n==st.st_nlink)
  record.update(root=str(root),closure_before=before,closure_immediately_before_removal=closure(root),current_inventory_matches=True)
  report['pending_verified_removal']=record;save()
  shutil.rmtree(root);assert not root.exists();record['released_allocated_bytes']+=allocated
 record['archive_closure']=closure(archive)
 allocated=archive.stat().st_blocks*512
 report['pending_verified_removal']=record;save()
 archive.unlink();record['released_allocated_bytes']+=allocated
 report.pop('pending_verified_removal',None);report['removed'].append(record);save()
 print('REMOVED',proof.name,record['released_allocated_bytes'],flush=True)
report['released_allocated_bytes']=sum(x['released_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['released_allocated_bytes'],flush=True)
