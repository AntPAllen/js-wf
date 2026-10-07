from storage_review_common import *
os.environ['AWS_ACCESS_KEY_ID']='x';os.environ['AWS_SECRET_ACCESS_KEY']='x'
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
root=Path('/tmp/js-wf-parallel-recovery-journal-24h-20261007')
proof=repo/'docs/scale/parallel-recovery-journal-24h-2026-10-07/terminal-failure'
out=repo/'docs/scale/tmp-storage-review-2026-10-07/failed24h-local-retirement';out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
def committed(name):
 p=proof/name;b=subprocess.check_output(['git','show',head+':'+str(p.relative_to(repo))],cwd=repo);assert b==p.read_bytes();return b
meta=json.loads(committed('archive-verification.json'));receipt=json.loads(committed('s3-readback.json'));invbytes=committed('fixture-inventory.json');inv=json.loads(invbytes)
assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
report=dict(head=head,root=str(root),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_before=shutil.disk_usage('/tmp').free,scope='Retire closed failed original24h only after fresh complete S3 member/hash readback and unchanged local inventory. Failed verdict preserved. Restore to a fresh copy for future diagnosis; never reopen original stores.')
def save(): (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
raw=subprocess.check_output(['systemctl','show','js-wf-parallel-recovery-journal-24h-20261007.service','-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'],text=True)
f=dict(line.split('=',1) for line in raw.splitlines());assert f==dict(MainPID='0',ExecMainPID='3743034',ExecMainStatus='1',Result='exit-code',InvocationID='cb8aebddd6d44f2b947fbb9604ada1da'),f
report['original_unit_properties']=raw;report['closure_before']=closure(root);save()
assert fixture_archive.inventory(root)==inv['files'];report['current_inventory_matches']=True;save()
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']);assert receipt['archive']['full_readback']==expected
print('VERIFY_REMOTE_FULL_ARCHIVE',flush=True)
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(s3.credentials());p.stdin.close()
 try: declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 err=p.stderr.read();assert p.wait()==0,err
assert declared==inv
report['remote_full_readback']=actual;report['remote_all_members_match']=True
for key,name in [('metadata','archive-verification.json'),('inventory','fixture-inventory.json')]:
 r=subprocess.run(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail',receipt[key]['url']],input=s3.credentials(),stdout=subprocess.PIPE,check=True)
 assert r.stdout==committed(name)
report['remote_metadata_and_inventory_match']=True;save()
inodes={};dirs=0
for path in [root,*root.rglob('*')]:
 st=path.stat()
 if path.is_dir():dirs+=st.st_blocks*512;continue
 k=(st.st_dev,st.st_ino)
 if k not in inodes:inodes[k]=[st,0]
 inodes[k][1]+=1
allocated=dirs+sum(st.st_blocks*512 for st,n in inodes.values() if n==st.st_nlink)
source=root/'source'
assert subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()=='bc9f92bdfd1f01ad78d4acd24e6576d46c82a078'
report['closure_immediately_before_removal']=closure(root);report['verified_pending_removal']=True;save()
subprocess.run(['git','worktree','remove',str(source)],cwd=repo,check=True)
report['source_worktree_removed_cleanly']=True;save()
closure(root);shutil.rmtree(root);assert not root.exists()
report.update(removed=True,released_allocated_bytes=allocated,free_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());save()
print('REMOVED',allocated,flush=True)
