import pathlib,json,hashlib,subprocess,os,shutil,datetime
repo=pathlib.Path('/home/exedev/js-wf');out=repo/'docs/scale/accepted-proof-duplicate-recovery-2026-10-04';head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert head==remote

def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()

def git_parts(proof):
 m=json.loads((proof/'manifest.json').read_text());combined=hashlib.sha256()
 for part in m['parts']:
  p=proof/part['path'];assert sha(p)==part['sha256']
  proc=subprocess.Popen(['git','show',head+':'+str(p.relative_to(repo))],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();count=0
  for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);combined.update(b);count+=len(b)
  assert proc.wait()==0 and h.hexdigest()==part['sha256'] and count==part['bytes']
 assert combined.hexdigest()==m['archive_sha256'];return m['archive_sha256']

root=pathlib.Path('/dev/shm/js-wf-tier2-current-journal97-108-37149506857');proof=repo/'docs/scale/current-tier2-matrix-2026-10-04/journal-97-108';m=json.loads((proof/'manifest.json').read_text());targets=[dict(path=root/'artifact',files={n.removeprefix('artifact/'):v['sha256'] for n,v in m['files'].items() if n.startswith('artifact/')},canonical=root/'proof.tar.gz',canonical_sha=m['archive_sha256'],proof=proof)]
root=pathlib.Path('/tmp/js-wf-clock-seed6-success-37170797762');proof=repo/'docs/scale/worker-clock-checkpoint-2026-10-04/accepted-seed-6';m=json.loads((proof/'manifest.json').read_text());targets.append(dict(path=root/'restored',files=json.loads((root/'original-stores/rolling-manifest.json').read_text())['files'],canonical=root/'original-stores/rolling-originals.tar.gz',canonical_sha=m['files']['original-stores/rolling-originals.tar.gz']['sha256'],proof=proof))
results=[]
for target in targets:
 p=target['path'];assert p.is_dir() and not p.is_symlink();actual={str(f.relative_to(p)):f for f in p.rglob('*') if f.is_file()};assert set(actual)==set(target['files']);assert not any(f.is_symlink() for f in p.rglob('*'))
 for name,f in actual.items():assert sha(f)==target['files'][name],name
 assert sha(target['canonical'])==target['canonical_sha'];proof_sha=git_parts(target['proof'])
 open_fds=[]
 for fd_dir in pathlib.Path('/proc').glob('[0-9]*/fd'):
  try:fds=list(fd_dir.iterdir())
  except (PermissionError,FileNotFoundError):continue
  for fd in fds:
   try:link=os.readlink(fd)
   except (PermissionError,FileNotFoundError):continue
   if link==str(p) or link.startswith(str(p)+'/'):open_fds.append(str(fd))
 assert not open_fds,open_fds
 results.append(dict(path=str(p),files=len(actual),raw_bytes=sum(f.stat().st_size for f in actual.values()),allocated_bytes=sum(f.stat().st_blocks*512 for f in actual.values()),all_expanded_member_sha_verified=True,canonical_sha256=target['canonical_sha'],canonical_path=str(target['canonical']),git_proof_archive_sha256=proof_sha,all_committed_parts_sha_verified=True,open_fds=open_fds))
# All targets verified before deletion; retain compressed originals and Git proof.
for target in targets:shutil.rmtree(target['path'])
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),proof_commit=head,pushed_main_verified=True,removed_only_accepted_duplicate_expansions=True,failed_evidence_unchanged=True,stores_reopened=False,targets=results,total_raw_bytes=sum(r['raw_bytes'] for r in results),total_allocated_bytes=sum(r['allocated_bytes'] for r in results))
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
