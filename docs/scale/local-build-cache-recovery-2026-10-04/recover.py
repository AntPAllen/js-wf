import pathlib,subprocess,os,json,datetime
root=pathlib.Path(__file__).parent;cache=pathlib.Path('/dev/shm/js-wf-go-build-cache-20261003');assert cache.is_dir() and not cache.is_symlink()
env=dict(os.environ,GOCACHE=str(cache),GOMAXPROCS='2',GOMEMLIMIT='512MiB')
assert subprocess.check_output(['go','env','GOCACHE'],env=env,text=True).strip()==str(cache)
processes=subprocess.run(['pgrep','-x','go|compile|link'],capture_output=True,text=True);assert processes.returncode==1,processes.stdout
opened=[]
for fds in pathlib.Path('/proc').glob('[0-9]*/fd'):
 try:items=list(fds.iterdir())
 except (FileNotFoundError,PermissionError):continue
 for fd in items:
  try:p=os.readlink(fd)
  except (FileNotFoundError,PermissionError):continue
  if p==str(cache) or p.startswith(str(cache)+'/'):opened.append(str(fd))
assert not opened,opened
def inventory():
 inodes={}
 for p in cache.rglob('*'):
  if p.is_file():
   s=p.stat();inodes[(s.st_dev,s.st_ino)]=s.st_blocks*512
 return dict(files=len(inodes),allocated_file_bytes=sum(inodes.values()))
before=inventory();p=subprocess.run(['go','clean','-cache'],env=env,text=True,capture_output=True);assert p.returncode==0,p.stderr;after=inventory()
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),cache=str(cache),command=['go','clean','-cache'],before=before,after=after,allocated_file_bytes_recovered=before['allocated_file_bytes']-after['allocated_file_bytes'],open_fds=opened,no_local_go_build_processes=True,retained_proofs_and_executables_outside_cache_untouched=True,source_or_qualification_changed=False)
(root/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
