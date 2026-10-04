import pathlib,json,hashlib,subprocess,tarfile,shutil,os,datetime
repo=pathlib.Path('/home/exedev/js-wf');out=repo/'docs/scale/tier2-first48-duplicate-recovery-2026-10-04';head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert head==remote

def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
results=[];targets=[]
for span in ['1-12','13-24','25-36','37-48']:
 proof=repo/'docs/scale/current-tier2-matrix-2026-10-03'/('journal-'+span);manifest=json.loads((proof/'manifest.json').read_text());root=pathlib.Path('/dev/shm')/('js-wf-tier2-current-journal'+span+'-37149506857');target=root/'artifact';assert target.is_dir() and not target.is_symlink();canonical=root/'proof.tar.gz';assert not canonical.exists()
 tmp=root/'proof.tar.gz.tmp';proc=subprocess.Popen(['git','show',head+':'+str((proof/'originals.tar.gz').relative_to(repo))],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();count=0
 with tmp.open('wb') as f:
  for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);f.write(b);count+=len(b)
 assert proc.wait()==0 and h.hexdigest()==manifest['archive_sha256'] and count==manifest['archive_bytes'];assert sha(tmp)==manifest['archive_sha256'];seen=set()
 with tarfile.open(tmp,'r:gz') as tar:
  for m in tar:
   if m.isdir():continue
   assert m.isfile() and m.name not in seen;seen.add(m.name);f=tar.extractfile(m);digest=hashlib.sha256()
   for b in iter(lambda:f.read(1024*1024),b''):digest.update(b)
   assert digest.hexdigest()==manifest['files'][m.name],m.name
 assert seen==set(manifest['files']) and len(seen)==manifest['archive_members'];tmp.rename(canonical)
 actual={str(p.relative_to(target)):p for p in target.rglob('*') if p.is_file()};expected={n.removeprefix('artifact/'):d for n,d in manifest['files'].items() if n.startswith('artifact/')};assert set(actual)==set(expected) and len(actual)==72;assert not any(p.is_symlink() for p in target.rglob('*'))
 for n,p in actual.items():assert sha(p)==expected[n],n
 openfds=[]
 for fd_dir in pathlib.Path('/proc').glob('[0-9]*/fd'):
  try:fds=list(fd_dir.iterdir())
  except (PermissionError,FileNotFoundError):continue
  for fd in fds:
   try:link=os.readlink(fd)
   except (PermissionError,FileNotFoundError):continue
   if link==str(target) or link.startswith(str(target)+'/'):openfds.append(str(fd))
 assert not openfds,openfds
 results.append(dict(range=span,path=str(target),raw_files=len(actual),raw_bytes=sum(p.stat().st_size for p in actual.values()),allocated_bytes=sum(p.stat().st_blocks*512 for p in actual.values()),canonical_path=str(canonical),canonical_sha256=manifest['archive_sha256'],canonical_bytes=count,canonical_members=len(seen),all_canonical_and_expanded_member_sha_verified=True,git_blob_and_local_archive_sha_verified=True,open_fds=openfds));targets.append(target)
# Verify all four targets before removing any accepted expansion.
for target in targets:shutil.rmtree(target)
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),proof_commit=head,pushed_main_verified=True,targets=results,recovered_allocated_bytes=sum(r['allocated_bytes'] for r in results),raw_bytes=sum(r['raw_bytes'] for r in results),compressed_local_and_git_proofs_retained=True,failed_evidence_unchanged=True,stores_reopened=False)
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
