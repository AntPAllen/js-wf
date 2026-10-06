from pathlib import Path
import subprocess,json,hashlib,os,sys,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','show',head+':scripts/fixture_delta.py'],cwd=repo)
paths=['tier2-retained-fanout-2026-10-06/native-ten-minute','tier2-retained-fanout-2026-10-06/copied-audit','tier2-retained-upgrade-2026-10-06/native-ten-minute','tier2-retained-upgrade-2026-10-06/copied-audit','tier2-retained-server-clock-2026-10-06/positive-native-ten-minute','tier2-retained-server-clock-2026-10-06/positive-physical-copied-audit']
out=repo/'docs/scale/recent-canonical-proof-headroom-2026-10-06';out.mkdir(exist_ok=True)
(out/'executed-recovery.py').write_bytes(Path(__file__).read_bytes())
archives=[];records=[]
for directory in paths:
 metadata='docs/scale/'+directory+'/archive-verification.json'
 members=fixture_delta.read_base(repo,head,metadata)
 manifest=json.loads(subprocess.check_output(['git','show',head+':'+metadata],cwd=repo))
 archives.append(dict(metadata=metadata,members=len(members)+1,archive_sha256=manifest['archive_sha256'],archive_bytes=manifest['archive_bytes'],all_canonical_parts_concat_members_manifest_verified=True))
 for part in manifest['parts']:
  name=str(Path(metadata).parent/part['file']);p=repo/name
  if not p.exists():continue
  assert p.is_file() and not p.is_symlink()
  data=p.read_bytes();assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256']
  assert subprocess.check_output(['git','cat-file','blob',head+':'+name],cwd=repo)==data
  blob=subprocess.check_output(['git','rev-parse',head+':'+name],cwd=repo,text=True).strip()
  assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==blob
  records.append(dict(path=name,bytes=len(data),allocated_bytes=p.stat().st_blocks*512,sha256=part['sha256'],git_blob=blob))
 assert archives[-1]['members']==manifest['members']
 print('VERIFIED',directory,flush=True)
selected={str(repo/record['path']) for record in records}
limits=[]
for process in Path('/proc').glob('[0-9]*'):
 for task in process.glob('task/*'):
  try: fds=list((task/'fd').iterdir())
  except (OSError,PermissionError):limits.append(str(task/'fd'));continue
  for fd in fds:
   try:target=os.readlink(fd)
   except FileNotFoundError:continue
   except PermissionError:limits.append(str(fd));continue
   assert target not in selected,('open proof part',str(fd),target)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before)
patterns.write_text(before+''.join('!/'+record['path']+'\n' for record in records))
subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for record in records:
 assert not (repo/record['path']).exists()
 assert subprocess.check_output(['git','rev-parse',head+':'+record['path']],cwd=repo,text=True).strip()==record['git_blob']
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),archives=archives,parts=records,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),visible_task_fds_checked=True,unobservable_descriptors=limits,free_bytes=shutil.disk_usage(repo).free,scope='Only canonical pushed proof worktree duplicates excluded. Canonical Git/full raw archives/donors/original fixtures/source/exes/caches/live stores retained. Visible descriptor limits recorded.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
