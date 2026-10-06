from pathlib import Path
import subprocess,json,hashlib,tarfile,os,shutil,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-parallel-latency-cohort-20261006');clone=root/'copied-stores'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base='docs/scale/parallel-final-latency-2026-10-06/retained-cohort/'
meta=json.loads(subprocess.check_output(['git','show',head+':'+base+'archive-verification.json'],cwd=repo));assert meta['all_archive_members_and_parts_read_back']
digest=hashlib.sha256();size=0
for part in meta['parts']:
 b=subprocess.check_output(['git','cat-file','blob',head+':'+base+part['file']],cwd=repo)
 assert len(b)==part['bytes'] and hashlib.sha256(b).hexdigest()==part['sha256'];digest.update(b);size+=len(b)
assert digest.hexdigest()==meta['archive_sha256'] and size==meta['archive_bytes']
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert sha(root/'proof.tar.gz')==meta['archive_sha256']
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and not Path('/proc/'+str(e['pid'])).exists()
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for server in servers:assert not Path('/proc/'+str(server['host_pid'])).exists()
manifest=json.loads((root/'archive-manifest.json').read_text())
with tarfile.open(root/'proof.tar.gz','r|gz') as t:
 for member in t:
  if member.name=='archive-manifest.json':assert json.load(t.extractfile(member))==manifest
actual={str(p.relative_to(root)):p for p in clone.rglob('*') if p.is_file()}
expected={name:item for name,item in manifest.items() if name.startswith('copied-stores/')}
assert set(actual)==set(expected)
for name,p in actual.items():assert p.stat().st_size==expected[name]['bytes'] and sha(p)==expected[name]['sha256']
for proc in Path('/proc').glob('[0-9]*'):
 for fd in proc.glob('task/*/fd/*'):
  try:target=os.readlink(fd)
  except (OSError,PermissionError):continue
  assert not target.startswith(str(clone)+'/' ),('open copied store',str(fd),target)
records=[{'path':name,**expected[name]} for name in sorted(expected)]
freed=sum(p.stat().st_size for p in actual.values());shutil.rmtree(clone)
out=repo/'docs/scale/parallel-final-latency-2026-10-06/reclaimed-admission-copy';out.mkdir()
(out/'executed-reclaim.py').write_bytes(Path(__file__).read_bytes())
(out/'reclamation.json').write_text(json.dumps({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head':head,'pushed_main_matches':True,'canonical_parts_concat_current_archive_verified':True,'archive_manifest_matches':True,'sdk_and_five_servers_closed':True,'all_visible_task_fds_checked':True,'files':len(records),'bytes_removed':freed,'removed':records,'free_bytes':shutil.disk_usage(repo).free,'scope':'Only fully preserved verified closed failed disposable copied stores; original donor/source/executables/full Git proof/cache/live campaigns retained'},indent=2)+'\n')
print('RECLAIMED',len(records),freed,flush=True)
