from pathlib import Path
import subprocess,json,hashlib,io,tarfile,os,datetime
repo=Path('/home/exedev/js-wf')
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert head==remote
pairs=[('direct-r1-r5-process-owner-loss','r5-process-owner-loss'),('direct-r1-r5-process-replay-diagnostic','r5-replay-diagnostic'),('direct-r1-r5-independent-cleanup','r5-independent-cleanup')]
report={'head':head,'remote_main':remote,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed':[]}
for rootname,dirname in pairs:
 root=Path('/tmp/js-wf-'+rootname+'-20261005');prefix='docs/scale/direct-callback-audit-2026-10-05/'+dirname+'/'
 def blob(name):return subprocess.check_output(['git','show',head+':'+prefix+name],cwd=repo)
 meta=json.loads(blob('archive-verification.json'));assert meta['all_archive_members_and_parts_read_back']
 e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
 records=json.loads((root/'actual-containers/actual-servers.json').read_text())
 for r in records:assert not Path('/proc/'+str(r['host_pid'])).exists()
 archive=root/'proof.tar.gz'
 with archive.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==meta['archive_sha256']
 data=[]
 for part in meta['parts']:
  b=blob(part['file']);assert len(b)==part['bytes'] and hashlib.sha256(b).hexdigest()==part['sha256'];data.append(b)
 joined=b''.join(data);assert len(joined)==meta['archive_bytes'] and hashlib.sha256(joined).hexdigest()==meta['archive_sha256']
 with tarfile.open(fileobj=io.BytesIO(joined)) as t:
  manifest=json.load(t.extractfile('archive-manifest.json'));assert len(t.getmembers())==meta['members']==len(manifest)+1
  for name,m in manifest.items():
   b=t.extractfile(name).read();assert len(b)==m['bytes'] and hashlib.sha256(b).hexdigest()==m['sha256']
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:tasks=list((proc/'task').iterdir())
  except (FileNotFoundError,PermissionError):continue
  for task in tasks:
   try:fds=list((task/'fd').iterdir())
   except (FileNotFoundError,PermissionError):continue
   for fd in fds:
    try:target=os.readlink(fd)
    except (FileNotFoundError,PermissionError):continue
    assert target!=str(archive),('archive open',fd,target)
 size=archive.stat().st_size;archive.unlink()
 report['removed'].append({'path':str(archive),'bytes':size,'sha256':meta['archive_sha256'],'pushed_parts_and_all_members_verified':True,'observed_processes_closed':len(records)+1,'original_stores_retained':True})
out=repo/'docs/scale/direct-callback-audit-2026-10-05/reclaimed-r5-proof-duplicates';out.mkdir()
(out/'reclamation.json').write_text(json.dumps(report,indent=2)+'\n');(out/'executed-reclaim.py').write_text(Path(__file__).read_text())
print(json.dumps(report))
