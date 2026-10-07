import sys,json,subprocess,os
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure
worktrees=[Path(s.removeprefix('worktree ')) for s in subprocess.check_output(['git','worktree','list','--porcelain'],text=True).splitlines() if s.startswith('worktree ') and Path(s.removeprefix('worktree ')).exists()]
rows=[];skip=[]
for root in sorted(Path('/tmp').iterdir()):
 if not root.is_dir() or root.is_symlink() or not root.name.startswith(('js-wf-','Test','wf-scale')):continue
 if 'candidate-partition200' in root.name:continue
 files=list(root.rglob('*'))
 size=sum(p.lstat().st_blocks*512 for p in files)+root.stat().st_blocks*512
 if size<1048576:continue
 try:
  assert not any(w.is_relative_to(root) or root.is_relative_to(w) for w in worktrees),'registered worktree'
  assert not any(p.is_symlink() or not(p.is_file() or p.is_dir()) for p in files),'symlink or special file'
  check=closure(root)
 except (AssertionError,ValueError) as e:
  skip.append(dict(root=str(root),bytes=size,reason=str(e)));continue
 rows.append(dict(root=str(root),allocated_bytes=size,closure=check))
Path('/tmp/tmp-closed-batch-plan.json').write_text(json.dumps(dict(selected=rows,skipped=skip),indent=2)+'\n')
print(json.dumps(dict(selected=len(rows),allocated_bytes=sum(r['allocated_bytes'] for r in rows),skipped=[{k:r[k] for k in ('root','bytes','reason')} for r in skip]),indent=2))
