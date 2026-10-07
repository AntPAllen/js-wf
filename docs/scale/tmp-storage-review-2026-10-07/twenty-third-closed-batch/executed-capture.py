import sys,json,os,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
plan=json.loads(Path('/tmp/tmp-closed-batch-plan.json').read_text())
stage=Path('/tmp/closed-tmp-batch-stage-20261007');stage.mkdir()
out=repo/'docs/scale/tmp-storage-review-2026-10-07/twenty-third-closed-batch'
out.mkdir()
shutil.copyfile('/tmp/plan_tmp_closed_batch.py',out/'executed-plan.py')
shutil.copyfile(__file__,out/'executed-capture.py')
shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
(out/'plan.json').write_text(json.dumps(plan,indent=2)+'\n')
for row in plan['selected']:
 root=Path(row['root']);closure(root)
 row['inventory']=fixture_archive.inventory(root)
 for name in row['inventory']:
  source=root/name;target=stage/root.name/name
  target.parent.mkdir(parents=True,exist_ok=True)
  assert source.stat().st_nlink==1,(source,'preexisting hardlinks')
  os.link(source,target)
 assert fixture_archive.inventory(root)==row['inventory']
proof=fixture_archive.capture(stage,Path('/tmp/closed-tmp-batch-20261007.tar.gz'),out,compresslevel=1)
for row in plan['selected']:
 root=Path(row['root']);row['closure_after']=closure(root)
 assert fixture_archive.inventory(root)==row['inventory']
(out/'origins.json').write_text(json.dumps(plan,indent=2)+'\n')
print(json.dumps(proof),flush=True)
