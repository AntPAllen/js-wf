import sys, importlib.util, json, shutil, datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('closure_source','/tmp/cleanup-evidence-storage-20261008.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
base=repo/'docs/scale/tmp-storage-review-2026-10-08/small-leftovers'
stage=Path('/tmp/tmp-small-leftovers-stage-20261008');archive=Path('/tmp/tmp-small-leftovers-20261008.tar.gz')
names=['operator-leaf-guard-staging-20261007','wf-cas-failure-inspection','wf-cas-bench-1138390010','js-wf-graph-reader-resume-pins-20261008','js-wf-graph-catalog-pins-20261008','js-wf-graph-worker-pins-20261008','js-wf-graph-reader-pins-20261008','js-wf-graph-journal-pins-20261008','js-wf-graph-application-pins-20261008']
roots=[Path('/tmp')/n for n in names]
if sys.argv[1]=='closure':
 print(json.dumps(m.closure(roots+[archive,stage])));sys.exit()
assert not base.exists() and not archive.exists() and not stage.exists()
closure=m.closure(roots);assert not closure['blocked'] and not closure['permission_limits'],closure
for root in roots:
 assert root.is_dir() and not root.is_symlink()
 for p in [root,*root.rglob('*')]:
  assert p.name!='.git' and not p.is_symlink() and (p.is_file() or p.is_dir()),p
  assert not p.is_file() or p.stat().st_nlink==1,p
base.mkdir(parents=True);stage.mkdir()
for root in roots:shutil.copytree(root,stage/root.name,copy_function=shutil.copy2)
proof=fixture_archive.capture(stage,archive,base,compresslevel=1)
files=json.loads((base/'fixture-inventory.json').read_text())['files']
for root in roots:
 expected={k[len(root.name)+1:]:v for k,v in files.items() if k.startswith(root.name+'/')}
 assert fixture_archive.inventory(root)==expected
shutil.rmtree(stage)
(base/'capture.json').write_text(json.dumps(dict(roots=[str(r) for r in roots],archive=str(archive),closure=closure,utc=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-capture.py')
(base/'README.md').write_text('# Remaining small temporary fixtures\n\nStorage preservation of inactive test stores, generated graph fixtures, and operator test staging. No test verdict changes. Git worktrees, active tool state, and the original attachment are retained. Full archive and per-file inventory are verified before local removal.\n')
print(json.dumps(proof))
