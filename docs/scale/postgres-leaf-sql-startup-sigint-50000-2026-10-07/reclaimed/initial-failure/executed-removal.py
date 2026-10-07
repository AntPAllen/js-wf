from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
config=s3.credentials();head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(path):
 data=subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo);assert (repo/path).read_bytes()==data;return data
root=Path('/tmp/js-wf-postgres-leaf-sql-startup-sigint-50000-20261007');archive=root.with_suffix('.tar.gz');proof=Path('docs/scale/postgres-leaf-sql-startup-sigint-50000-2026-10-07/initial-failure')
out=Path('/tmp/storage-sql-leaf-startup-sigint-failure-removal-20261007');out.mkdir();shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report=dict(head=head,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_before=shutil.disk_usage('/tmp').free,scope='Original failed SQL startup SIGINT fixture/archive (no full50000 phase accepted) and its exact owned stopped PostgreSQL container/volume only; fresh S3 every-member/current-inventory/process/FD/Docker/mount/loop checks. Live24h and all other fixtures retained.')
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free;(out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'));invbytes=blob(proof/meta['inventory_file']);inv=json.loads(invbytes)
assert hashlib.sha256(invbytes).hexdigest()==meta['inventory_sha256']
expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']};assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
review=json.loads(blob(proof/'independent-review.json'));assert review['unit']['ExecMainStatus']=='1' and review['verdict'].startswith('Failed: SQL startup residual session count1')
assert json.loads(blob(proof/'execution.json'))['exit_code']==1
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if s.startswith('worktree ')];assert not any(w.is_relative_to(root) for w in worktrees)
unit=subprocess.check_output(['systemctl','show',root.name+'.service','-p','LoadState','-p','SubState','-p','MainPID','-p','ExecMainStatus','-p','ExecMainPID','-p','InvocationID','-p','Restart'],text=True);fields=dict(line.split('=',1) for line in unit.splitlines());assert fields['LoadState']=='loaded' and fields['SubState']=='failed' and fields['MainPID']=='0' and fields['ExecMainStatus']=='1' and fields['Restart']=='no'
assert json.loads((root/'actual-sdk.json').read_text())['stat'].split(') ',1)[1].split()[1]==fields['ExecMainPID']
report.update(metadata=str(proof/'archive-verification.json'),receipt=str(proof/'s3-readback.json'),terminal_unit=unit,closure_before=closure(root));assert fixture_archive.inventory(root)==inv['files'];save()
print('VERIFY_REMOTE',flush=True)
with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
 p.stdin.write(config);p.stdin.close()
 try:declared,actual=fixture_archive.verify_hashed_stream(p.stdout,expected)
 except BaseException:p.kill();p.wait();raise
 error=p.stderr.read();assert p.wait()==0,error
assert declared==inv and fixture_archive.inventory(root)==inv['files']
assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
with archive.open('rb') as f:assert s3.digest(f)==expected
pg=json.loads((root/'actual-postgres.json').read_text());stopped=json.loads((root/'postgres-stopped.json').read_text());media=json.loads((root/'postgres-media-copy-verification.json').read_text())
container=root.name+'-postgres';volume=container+'-data'
observed=json.loads(subprocess.check_output(['docker','inspect',container]))[0]
assert observed['Id']==stopped['Id']==pg['container']['Id'] and observed['Name']=='/'+container and not observed['State']['Running'] and observed['State']['Pid']==0
mounts=observed['Mounts'];assert len(mounts)==1 and mounts[0]['Type']=='volume' and mounts[0]['Name']==volume and mounts[0]['Destination']=='/var/lib/postgresql/data'
vol=json.loads(subprocess.check_output(['docker','volume','inspect',volume]))[0];mount=Path(vol['Mountpoint']);assert media['volume']==volume and mount==Path(mounts[0]['Source'])
all_ids=subprocess.check_output(['docker','ps','-aq'],text=True).split();all_containers=json.loads(subprocess.check_output(['docker','inspect',*all_ids]))
assert not any(c['Id']!=observed['Id'] and any(m.get('Name')==volume for m in c['Mounts']) for c in all_containers)
check_code="import sys,pathlib,hashlib,json; r=pathlib.Path(sys.argv[1]); f={str(p.relative_to(r)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in r.rglob('*') if p.is_file()}; print(json.dumps({'files':f,'allocated_bytes':r.stat().st_blocks*512+sum(p.stat().st_blocks*512 for p in r.rglob('*'))}))"
source=json.loads(subprocess.check_output(['sudo','-n','python3','-c',check_code,str(mount)]));assert source['files']==media['files']
for name,digest in source['files'].items():
 path=root/'postgres-stopped-data'/name
 with path.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==digest
report.update(remote_archive_every_member_and_hash=actual,files=len(inv['files']),closure_immediately_before_removal=closure(root),archive_closure=closure(archive),owned_container=observed,owned_volume=vol,full_closed_volume_files_match_archived_copy=True,volume_files=len(source['files']),removed_volume_allocated_bytes=source['allocated_bytes'],removed_fixture_allocated_bytes=sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512,removed_archive_allocated_bytes=archive.stat().st_blocks*512)
report['pending_verified_removal']=True;save()
subprocess.run(['docker','rm',observed['Id']],check=True,stdout=subprocess.DEVNULL);report['owned_container_removed']=True;save()
subprocess.run(['docker','volume','rm',volume],check=True,stdout=subprocess.DEVNULL);report['owned_volume_removed']=True;save()
shutil.rmtree(root);archive.unlink();report.pop('pending_verified_removal');report['root_and_archive_removed']=True
report['removed_allocated_bytes']=report['removed_volume_allocated_bytes']+report['removed_fixture_allocated_bytes']+report['removed_archive_allocated_bytes'];report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save();print('FINISHED',report['removed_allocated_bytes'],report['free_bytes_after'],flush=True)
