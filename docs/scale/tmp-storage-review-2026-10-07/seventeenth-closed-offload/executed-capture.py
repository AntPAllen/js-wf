import sys,json,subprocess,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
base=repo/'docs/scale/tmp-storage-review-2026-10-07/seventeenth-closed-offload'
base.mkdir(exist_ok=True)
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if s.startswith('worktree ')]
report={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'selected':[],'retained':[]}
scan=subprocess.run(['du','-x','-b','--max-depth=1','/tmp'],text=True,capture_output=True)
assert scan.returncode in (0,1)
report['size_scan_errors']=scan.stderr
for line in scan.stdout.splitlines():
 size,name=line.split('\t',1);root=Path(name)
 if root==Path('/tmp') or int(size)<10*1024*1024:continue
 try:
  assert not any(w.is_relative_to(root) or root.is_relative_to(w) for w in worktrees),'git worktree'
  c=closure(root)
  inv=fixture_archive.inventory(root)
 except Exception as e:
  report['retained'].append({'root':name,'bytes':int(size),'reason':repr(e)[:300]});continue
 label=root.name;out=base/label;out.mkdir()
 archive=Path('/tmp')/('seventeenth-'+label+'.tar.gz')
 (out/'closure-before.json').write_text(json.dumps(c,indent=2)+'\n')
 result=fixture_archive.capture(root,archive,out,compresslevel=1)
 (out/'closure-after.json').write_text(json.dumps(closure(root),indent=2)+'\n')
 origin={'root':name,'archive':str(archive),'qualification':'Closed storage preservation only. Original verdicts remain unchanged; restore into a fresh directory before reuse. No process started.'}
 (out/'origin.json').write_text(json.dumps(origin,indent=2)+'\n')
 report['selected'].append(origin)
 (base/'selection.json').write_text(json.dumps(report,indent=2)+'\n')
 print('CAPTURED',label,result['archive_bytes'],flush=True)
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(base/'selection.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-capture.py');shutil.copyfile('/tmp/storage_review_common.py',base/'executed-common.py')
print('SELECTED',len(report['selected']),'RETAINED',len(report['retained']),flush=True)
