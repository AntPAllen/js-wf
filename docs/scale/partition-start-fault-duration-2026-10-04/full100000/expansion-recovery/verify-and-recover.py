import pathlib,hashlib,json,subprocess,tarfile,os,shutil
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-partition-start-full100000-20261004');raw=root/'stores'
out=pathlib.Path('/tmp/js-wf-partition-start-store-expansion-recovery-20261004');out.mkdir(exist_ok=True)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head
proofdir=repo/'docs/scale/partition-start-fault-duration-2026-10-04/full100000';manifest=json.loads((proofdir/'manifest.json').read_text());review=json.loads((root/'original-store-inventory.json').read_text())
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert len(review)==375 and json.loads((root/'independent-review.json').read_text())['source'] == '3b859aa6f7385b09792b6985156e8ef880108f27'
canonical=root/'proof.tar.gz';assert canonical.stat().st_size==manifest['archive_bytes'] and sha(canonical)==manifest['archive_sha256']
joined=hashlib.sha256();total=0
for part in manifest['parts']:
 path=str((proofdir/part['path']).relative_to(repo));proc=subprocess.Popen(['git','show',head+':'+path],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();count=0
 for b in iter(lambda:proc.stdout.read(1048576),b''):h.update(b);joined.update(b);count+=len(b)
 assert proc.wait()==0 and count==part['bytes'] and h.hexdigest()==part['sha256'];total+=count
assert total==manifest['archive_bytes'] and joined.hexdigest()==manifest['archive_sha256']
seen=set()
with tarfile.open(canonical,'r:gz') as t:
 for m in t:
  assert m.isfile() and m.name not in seen;seen.add(m.name);expected=manifest['files'][m.name];f=t.extractfile(m)
  assert m.size==expected['bytes'] and hashlib.file_digest(f,'sha256').hexdigest()==expected['sha256'],m.name
assert seen==set(manifest['files'])
files={str(p.relative_to(root)):p for p in raw.rglob('*') if p.is_file()};assert set(files)==set(review)
ledger={};inodes=set();blocks=0
for n,p in files.items():
 assert not p.is_symlink();d=sha(p);assert d==review[n]['sha256']==manifest['files'][n]['sha256']
 st=p.stat();assert st.st_nlink==1;key=(st.st_dev,st.st_ino);assert key not in inodes;inodes.add(key);blocks+=st.st_blocks*512
 ledger[n]=dict(sha256=d,bytes=st.st_size,allocated_bytes=st.st_blocks*512)
openfds=[]
for fds in pathlib.Path('/proc').glob('[0-9]*/fd'):
 try:items=list(fds.iterdir())
 except (PermissionError,FileNotFoundError):continue
 for fd in items:
  try:target=os.readlink(fd)
  except (PermissionError,FileNotFoundError):continue
  if target==str(raw) or target.startswith(str(raw)+'/'):openfds.append(dict(fd=str(fd),target=target))
assert not openfds,openfds
assert sha(root/'integration.test')==manifest['files']['integration.test']['sha256']
shutil.rmtree(raw)
assert not raw.exists() and canonical.exists() and sha(canonical)==manifest['archive_sha256'] and sha(root/'integration.test')==manifest['files']['integration.test']['sha256']
result=dict(pushed_head=head,removed_path=str(raw),files=ledger,exclusive_allocated_bytes_recovered=blocks,canonical_path=str(canonical),canonical_sha256=manifest['archive_sha256'],canonical_members_verified=len(seen),complete_raw_files_verified=len(files),all_git_parts_sha_verified=True,all_member_contents_verified=True,open_fds=openfds,actual_workload_binary_retained=True,failed_originals_untouched=True,qualification_unchanged=True)
(out/'recovery.json').write_text(json.dumps(result,indent=2)+'\n');shutil.copyfile(__file__,out/'verify-and-recover.py');print(json.dumps({k:v for k,v in result.items() if k!='files'},indent=2))
