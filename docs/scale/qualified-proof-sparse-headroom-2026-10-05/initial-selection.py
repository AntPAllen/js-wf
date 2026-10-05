import pathlib,subprocess,hashlib,json,os,datetime
repo=pathlib.Path('/home/exedev/js-wf');os.chdir(repo)
head=subprocess.check_output(['git','rev-parse','HEAD']).decode().strip()
remote=subprocess.check_output(['git','ls-remote','--heads','origin','main']).decode().split()[0]
assert head==remote
assert not subprocess.check_output(['git','status','--porcelain'])
patterns=subprocess.check_output(['git','sparse-checkout','list']).decode().splitlines()
roots=[repo/'docs/scale/current-tier2-matrix-2026-10-04',repo/'docs/scale/current-tier3-block-delay-2026-10-04']
chosen=[];manifests=[]
for root in roots:
 for directory in sorted(root.iterdir()):
  if not directory.is_dir() or not directory.name.startswith(('cluster-','consumer-','seeds-')):continue
  path=directory/'manifest.json'
  if not path.is_file():continue
  manifest=json.loads(path.read_text())
  assert manifest.get('all_members_readback_verified'),path
  for part in manifest['parts']:
   file=directory/part['path']
   if not file.is_file():continue
   assert 'proof.tar.gz.part' in file.name
   data=file.read_bytes();stat=file.stat();sha=hashlib.sha256(data).hexdigest()
   assert len(data)==part['bytes'] and sha==part['sha256'],file
   rel=str(file.relative_to(repo))
   blob=subprocess.check_output(['git','rev-parse',head+':'+rel]).decode().strip()
   physical_blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
   assert blob==physical_blob,file
   chosen.append({'path':rel,'bytes':len(data),'allocated_bytes':stat.st_blocks*512,'sha256':sha,'git_blob':blob,'manifest':str(path.relative_to(repo))})
  manifests.append(str(path.relative_to(repo)))
assert chosen
# Read the stored canonical bytes, independently of worktree copies.
reader=subprocess.Popen(['git','cat-file','--batch'],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
for entry in chosen:
 reader.stdin.write((entry['git_blob']+'\n').encode());reader.stdin.flush()
 header=reader.stdout.readline().decode().strip().split()
 assert header==[entry['git_blob'],'blob',str(entry['bytes'])],header
 h=hashlib.sha256();remaining=entry['bytes']
 while remaining:
  data=reader.stdout.read(min(1024*1024,remaining));assert data
  h.update(data);remaining-=len(data)
 assert reader.stdout.read(1)==b'\n'
 assert h.hexdigest()==entry['sha256']
reader.stdin.close();assert reader.wait()==0
fds=[];targets={str(repo/e['path']) for e in chosen}
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 try:handles=list((proc/'fd').iterdir())
 except OSError:continue
 for fd in handles:
  try:target=os.readlink(fd)
  except OSError:continue
  if target in targets:fds.append({'pid':proc.name,'target':target})
assert not fds,fds
before=os.statvfs(repo).f_bavail*os.statvfs(repo).f_frsize
addition=['!/'+entry['path'] for entry in chosen]
new_patterns=list(dict.fromkeys(patterns+addition))
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],input=('\n'.join(new_patterns)+'\n').encode(),check=True)
assert all(not (repo/e['path']).exists() for e in chosen)
assert not subprocess.check_output(['git','status','--porcelain'])
assert subprocess.check_output(['git','rev-parse','HEAD']).decode().strip()==head
# All recorded blobs must remain readable after applying sparse patterns.
check=subprocess.check_output(['git','cat-file','--batch-check'],input=('\n'.join(e['git_blob'] for e in chosen)+'\n').encode()).decode().splitlines()
assert len(check)==len(chosen)
for line,e in zip(check,chosen):assert line.split()==[e['git_blob'],'blob',str(e['bytes'])]
after=os.statvfs(repo).f_bavail*os.statvfs(repo).f_frsize
out=repo/'docs/scale/qualified-proof-sparse-headroom-2026-10-05';out.mkdir()
report={'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_and_remote_main':head,'parts':chosen,'part_count':len(chosen),'manifest_count':len(manifests),'allocated_worktree_bytes_recovered':sum(e['allocated_bytes'] for e in chosen),'available_before':before,'available_after':after,'part_manifest_worktree_git_sha256_verified':True,'all_git_objects_remain_readable':True,'worktree_clean_after_sparse_change':True,'visible_open_descriptors':fds,'existing_sparse_patterns_preserved':True,'scope':'accepted Tier2 all-server/consumer and Tier3 disk-delay part worktree copies only; Git canonical objects retained; no fixture, ZIP, canonical /tmp archive, failed or live originals removed'}
(out/'executed-sparse-recovery.json').write_text(json.dumps(report,indent=2)+'\n')
(out/'prior-patterns.txt').write_text('\n'.join(patterns)+'\n')
(out/'new-exclusions.txt').write_text('\n'.join(addition)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='parts'}))
