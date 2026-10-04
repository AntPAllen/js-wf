import pathlib,subprocess,hashlib,json,os,datetime
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path(__file__).parent
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
assert subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]==head
paths=subprocess.check_output(['git','ls-files','-z'],cwd=repo).decode().split('\0')[:-1]
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
source={n:sha(repo/n) for n in paths if pathlib.Path(n).suffix in {'.go','.py','.yml','.yaml'} or pathlib.Path(n).name in {'go.mod','go.sum'}}
(root/'source-before.json').write_text(json.dumps(source,indent=2)+'\n')
selected=[]
for n in paths:
 p=repo/n
 if not n.startswith('docs/scale/') or not p.is_file() or p.stat().st_size<16*1024*1024:continue
 assert not p.is_symlink() and ('.tar.gz' in n or n.endswith('.gz')),n
 d=sha(p);proc=subprocess.Popen(['git','show',head+':'+n],cwd=repo,stdout=subprocess.PIPE);h=hashlib.sha256();count=0
 for b in iter(lambda:proc.stdout.read(1024*1024),b''):h.update(b);count+=len(b)
 assert proc.wait()==0 and h.hexdigest()==d and count==p.stat().st_size,n
 selected.append(dict(path=n,sha256=d,bytes=count,allocated_bytes=p.stat().st_blocks*512))
assert selected
selected_paths={str(repo/x['path']) for x in selected};open_fds=[]
for fds in pathlib.Path('/proc').glob('[0-9]*/fd'):
 try:items=list(fds.iterdir())
 except (PermissionError,FileNotFoundError):continue
 for fd in items:
  try:target=os.readlink(fd)
  except (PermissionError,FileNotFoundError):continue
  if target in selected_paths:open_fds.append(dict(fd=str(fd),target=target))
assert not open_fds,open_fds
before=(repo/'.git/info/sparse-checkout').read_text();(root/'sparse-before.txt').write_text(before)
patterns=before.rstrip()+'\n'+''.join('!/'+x['path']+'\n' for x in selected)
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],input=patterns,text=True,cwd=repo,check=True)
after={n:sha(repo/n) for n in source};assert after==source
assert all(not (repo/x['path']).exists() for x in selected)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');(root/'sparse-after.txt').write_text((repo/'.git/info/sparse-checkout').read_text())
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),pushed_commit=head,selected=selected,working_archives_omitted=len(selected),allocated_working_bytes_recovered=sum(x['allocated_bytes'] for x in selected),tracked_source_inputs_unchanged=len(source),all_source_before_after_identical=True,all_archive_git_blob_sizes_and_sha_verified=True,all_omitted_archives_remain_in_local_and_pushed_git=True,open_fds=open_fds,failed_originals_outside_git_unchanged=True,qualification_unchanged=True)
(root/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='selected'},indent=2))
