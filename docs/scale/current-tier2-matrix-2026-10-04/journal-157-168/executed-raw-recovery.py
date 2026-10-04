import pathlib,json,hashlib,subprocess,shutil,os,sys,datetime
repo=pathlib.Path('/home/exedev/js-wf');span=sys.argv[1];assert span in {'157-168'};root=pathlib.Path('/dev/shm')/('js-wf-tier2-current-journal'+span+'-37149506857');proof=repo/'docs/scale/current-tier2-matrix-2026-10-04'/('journal-'+span);target=root/'artifact';reportpath=proof/'duplicate-removal.json';assert not reportpath.exists();head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert head==remote

def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
m=json.loads((proof/'manifest.json').read_text());expected={n.removeprefix('artifact/'):v['sha256'] for n,v in m['files'].items() if n.startswith('artifact/')};actual={str(f.relative_to(target)):f for f in target.rglob('*') if f.is_file()};assert target.is_dir() and not target.is_symlink() and set(actual)==set(expected) and len(actual)==72;assert not any(f.is_symlink() for f in target.rglob('*'))
for n,p in actual.items():assert sha(p)==expected[n],n
assert sha(root/'proof.tar.gz')==m['archive_sha256'];combined=hashlib.sha256()
for part in m['parts']:
 p=proof/part['path'];proc=subprocess.Popen(['git','show',head+':'+str(p.relative_to(repo))],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();size=0
 for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);combined.update(b);size+=len(b)
 assert proc.wait()==0 and h.hexdigest()==part['sha256'] and size==part['bytes']
assert combined.hexdigest()==m['archive_sha256'];openfds=[]
for fd_dir in pathlib.Path('/proc').glob('[0-9]*/fd'):
 try:fds=list(fd_dir.iterdir())
 except (PermissionError,FileNotFoundError):continue
 for fd in fds:
  try:link=os.readlink(fd)
  except (PermissionError,FileNotFoundError):continue
  if link==str(target) or link.startswith(str(target)+'/'):openfds.append(str(fd))
assert not openfds,openfds
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),range=span,proof_commit=head,pushed_main_verified=True,path=str(target),files=len(actual),raw_bytes=sum(f.stat().st_size for f in actual.values()),allocated_bytes=sum(f.stat().st_blocks*512 for f in actual.values()),all_expanded_sha_verified=True,canonical_path=str(root/'proof.tar.gz'),canonical_sha256=m['archive_sha256'],all_committed_parts_sha_verified=True,open_fds=openfds,compressed_local_and_git_proofs_retained=True,failed_evidence_unchanged=True)
shutil.copyfile(__file__,proof/'executed-raw-recovery.py');shutil.rmtree(target);reportpath.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
