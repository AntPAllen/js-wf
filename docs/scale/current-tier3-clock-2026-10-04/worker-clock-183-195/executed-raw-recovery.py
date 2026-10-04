import pathlib,json,hashlib,subprocess,os,shutil,datetime,tarfile
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-tier3-clock183-195-37164231641');target=root/'raw';out=repo/'docs/scale/current-tier3-clock-2026-10-04/worker-clock-183-195';head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head;m=json.loads((out/'manifest.json').read_text())
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
expected={n.removeprefix('raw/'):v['sha256'] for n,v in (m['files'] | m['external_files']).items() if n.startswith('raw/')};actual={str(p.relative_to(target)):p for p in target.rglob('*') if p.is_file()};assert target.is_dir() and not target.is_symlink() and set(actual)==set(expected);assert not any(p.is_symlink() for p in target.rglob('*'))
for n,p in actual.items():assert sha(p)==expected[n],n
assert sha(root/'compact-proof.tar.gz')==m['archive_sha256']
seen=set()
with tarfile.open(root/'compact-proof.tar.gz','r:gz') as tar:
 for member in tar:
  assert (member.isfile() or member.islnk()) and member.name not in seen
  seen.add(member.name);f=tar.extractfile(member);h=hashlib.sha256();count=0
  for data in iter(lambda:f.read(1024*1024),b''):h.update(data);count+=len(data)
  expected_member=m['files'][member.name];assert h.hexdigest()==expected_member['sha256'] and count==expected_member['bytes']
assert seen==set(m['files'])
models=json.loads((root/'history-model-proof.json').read_text());assert models['all_three_models_pass_every_seed'] and sha(root/'history-review')==models['binary_sha256']
restore_root=pathlib.Path('/tmp/js-wf-clock183-195-restoration-check')
for n,v in m['external_files'].items():assert sha(restore_root/n)==v['sha256']
combined=hashlib.sha256()
for part in m['parts']:
 p=out/part['path'];assert sha(p)==part['sha256'];proc=subprocess.Popen(['git','show',head+':'+str(p.relative_to(repo))],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();count=0
 for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);combined.update(b);count+=len(b)
 assert proc.wait()==0 and h.hexdigest()==part['sha256'] and count==part['bytes']
assert combined.hexdigest()==m['archive_sha256'];opened=[]
for fds in pathlib.Path('/proc').glob('[0-9]*/fd'):
 try:files=list(fds.iterdir())
 except (FileNotFoundError,PermissionError):continue
 for fd in files:
  try:link=os.readlink(fd)
  except (FileNotFoundError,PermissionError):continue
  if link==str(target) or link.startswith(str(target)+'/'):opened.append(str(fd))
assert not opened,opened
inodes={}
for p in actual.values():
 s=p.stat();key=(s.st_dev,s.st_ino)
 if key not in inodes:inodes[key]=dict(paths=0,links=s.st_nlink,allocated=s.st_blocks*512)
 inodes[key]['paths']+=1
exclusive=sum(v['allocated'] for v in inodes.values() if v['paths']==v['links']);shared=sum(v['allocated'] for v in inodes.values() if v['paths']<v['links'])
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),proof_commit=head,pushed_main_verified=True,target=str(target),files=len(actual),raw_apparent_bytes=sum(p.stat().st_size for p in actual.values()),unique_exclusive_file_allocated_bytes_recovered=exclusive,unique_shared_file_allocated_bytes_preserved_elsewhere=shared,all_expanded_member_sha_verified=True,all_canonical_and_committed_parts_sha_verified=True,canonical_path=str(root/'compact-proof.tar.gz'),canonical_sha256=m['archive_sha256'],open_fds=opened,actual_model_binary_retained=True,provider_and_git_workload_binaries_retained=True,failed_evidence_unchanged=True,new_store_archive_downloaded=False)
shutil.rmtree(target);assert not target.exists() and sha(root/'history-review')==models['binary_sha256'];(out/'duplicate-removal.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-raw-recovery.py');print(json.dumps(report,indent=2))
