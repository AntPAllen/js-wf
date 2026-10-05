from pathlib import Path
import subprocess,json,hashlib,os,shutil,sys
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-direct-callback-400k-copied-comparison-20261005');base='docs/scale/direct-callback-audit-2026-10-05/capacity-400k/'
out=repo/'docs/scale/direct-callback-audit-2026-10-05/reclaimed-copy'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
meta=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+base+'archive-verification.json'],cwd=repo))
h=hashlib.sha256();total=0
for part in meta['parts']:
 data=subprocess.check_output(['git','cat-file','blob',head+':'+base+part['file']],cwd=repo)
 assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256']
 h.update(data);total+=len(data)
assert h.hexdigest()==meta['archive_sha256'] and total==meta['archive_bytes']
def sha(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
assert sha(root/'proof-delta.tar.gz')==h.hexdigest()
sys.path.insert(0,str(repo/'scripts'));import fixture_delta
assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','cat-file','blob',head+':scripts/fixture_delta.py'],cwd=repo)
verified=fixture_delta.verify(root/'proof-delta.tar.gz',repo)
manifest=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+base+'lossless-manifest.json'],cwd=repo))
assert manifest==json.loads((root/'lossless-manifest.json').read_text())
execution=json.loads((root/'execution.json').read_text());assert execution['status']=='failed'
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for pid in [execution['pid']]+[x['host_pid'] for x in servers]:assert not Path('/proc/'+str(pid)).exists()
clone=root/'copied-stores';expected={n:info for n,info in manifest['files'].items() if n.startswith('copied-stores/')};actual={}
for path in clone.rglob('*'):
 assert not path.is_symlink()
 if path.is_file():
  name=path.relative_to(root).as_posix();assert name in expected
  assert sha(path)==expected[name]['sha256'] and path.stat().st_size==expected[name]['bytes']
  actual[name]=path.stat().st_size
assert set(actual)==set(expected)
open_fds=[]
for proc in Path('/proc').glob('[0-9]*'):
 for task in (proc/'task').glob('[0-9]*'):
  try:
   for fd in (task/'fd').iterdir():
    try:
     target=os.readlink(fd)
     if target==str(clone) or target.startswith(str(clone)+'/'):open_fds.append(str(fd))
    except OSError:pass
  except OSError:pass
assert not open_fds,open_fds
out.mkdir(parents=True)
report={'pushed_revision':head,'delta_archive_sha256':h.hexdigest(),'base':meta['base'],'complete_virtual_tree_verified':verified,'closed_sdk_and_five_servers':True,'all_visible_task_fds_checked':True,'copied_files':len(actual),'copied_bytes':sum(actual.values()),'scope':'Only closed disposable copied-stores removed after complete pushed canonical base-plus-delta and current copy verification; original stores and complete proof retained'}
shutil.rmtree(clone);assert not clone.exists()
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');(out/'executed-recovery.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(report),flush=True)
