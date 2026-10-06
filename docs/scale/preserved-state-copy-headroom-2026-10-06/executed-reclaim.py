from pathlib import Path
import subprocess,json,hashlib,sys,os,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','show',head+':scripts/fixture_delta.py'],cwd=repo)
cases=[('state-watch-leader-loss-copied','state-watch-leader-loss-2026-10-05'),('state-watch-mapped-client-copied','state-watch-leader-loss-2026-10-05/mapped-client-qualified'),('state-snapshot-copied','state-snapshot-copied-2026-10-05')]
out=repo/'docs/scale/preserved-state-copy-headroom-2026-10-06';out.mkdir(exist_ok=True)
(out/'executed-reclaim.py').write_bytes(Path(__file__).read_bytes())
report={'head':head,'pushed_main_matches':True,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[],'scope':'Only closed disposable copied-stores after full canonical archive/member/manifest/current-byte/SDK/container/visible-task-FD verification. Original donors, originals directories, source/exes/caches/canonical Git proof and live stores retained.'}
def save():
 report['allocated_bytes_recovered']=sum(x['allocated_bytes'] for x in report['removed']);report['free_bytes']=shutil.disk_usage(repo).free
 (out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for name,canonical in cases:
 root=Path('/tmp/js-wf-'+name+'-20261005');clone=root/'copied-stores';assert clone.is_dir() and not clone.is_symlink()
 metadata='docs/scale/'+canonical+'/archive-verification.json'
 expected=fixture_delta.read_base(repo,head,metadata)
 assert json.loads((root/'archive-manifest.json').read_text())==expected
 for leaf in ('execution.json','actual-containers/actual-servers.json'):
  p=root/leaf;assert dict(bytes=p.stat().st_size,sha256=sha(p))==expected[leaf]
 e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc',str(e['pid'])).exists()
 servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
 for server in servers:
  assert not Path('/proc',str(server['host_pid'])).exists()
  status=subprocess.run(['docker','inspect',server['container']['Id']],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  if status.returncode:assert 'no such object' in status.stderr.lower()
  else:assert not json.loads(status.stdout)[0]['State']['Running']
 actual={str(p.relative_to(root)):p for p in clone.rglob('*') if p.is_file()};selected={k:v for k,v in expected.items() if k.startswith('copied-stores/')};assert set(actual)==set(selected) and actual
 assert not any(p.is_symlink() for p in clone.rglob('*'))
 for key,p in actual.items():assert dict(bytes=p.stat().st_size,sha256=sha(p))==selected[key]
 running=subprocess.check_output(['docker','ps','-q'],text=True).split()
 if running:
  for container in json.loads(subprocess.check_output(['docker','inspect',*running],text=True)):
   for mount in container.get('Mounts',[]):assert not Path(mount.get('Source','')).is_relative_to(clone)
 for proc in Path('/proc').glob('[0-9]*'):
  for fd in proc.glob('task/*/fd/*'):
   try:target=os.readlink(fd)
   except (OSError,PermissionError):continue
   assert target!=str(clone) and not target.startswith(str(clone)+'/' ),('open copied store',str(fd),target)
 record={'root':str(root),'removed_path':str(clone),'canonical_metadata':metadata,'complete_canonical_archive_members_and_manifest_verified':True,'sdk_and_five_servers_closed':True,'recorded_containers_stopped_or_absent':True,'no_running_container_mounts_copies':True,'all_visible_task_fds_checked':True,'native_status_unchanged':e['status'],'files':len(actual),'bytes':sum(p.stat().st_size for p in actual.values()),'allocated_bytes':sum(p.stat().st_blocks*512 for p in actual.values()),'removed':[{'path':k,**v} for k,v in sorted(selected.items())]}
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
 shutil.rmtree(clone);report['removed'].append(record);save();print('RECLAIMED',name,record['allocated_bytes'],flush=True)
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
