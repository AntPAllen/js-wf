import sys,json,datetime,shutil,subprocess
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
names=['js-wf-candidate-seed31-diagnostics-20261007','js-wf-candidate-seed31-invocation-media-20261008','js-wf-candidate-partition200-seed001-independent-20261007','js-wf-candidate-partition200-seed002-independent-20261007','js-wf-candidate-partition200-seed003-independent-20261007','js-wf-candidate-partition200-v2-20261007']
base=repo/'docs/scale/tmp-storage-review-2026-10-08/closed-copies'
base.mkdir(exist_ok=True)
shutil.copyfile(__file__,base/'executed-capture.py')
shutil.copyfile('/tmp/storage_review_common.py',base/'executed-common.py')
rows=[]
for name in names:
 root=Path('/tmp')/name
 checks=closure(root)
 if (root/'source/.git').exists():
  assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
  revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root/'source',text=True).strip()
 else: revision=None
 archive=Path('/tmp')/(name+'-storage-oct8.tar.gz')
 proof=fixture_archive.capture(root,archive,base/name,compresslevel=1)
 rows.append(dict(root=str(root),archive=str(archive),canonical_metadata=str((base/name/'archive-verification.json').relative_to(repo)),source_revision=revision,closure_before=checks,closure_after=closure(root),archive_proof=proof))
 print(name,proof['archive_bytes'],flush=True)
(base/'capture.json').write_text(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),fixtures=rows,scope='Storage preservation only. Original failed/partial artifacts and verdicts unchanged.'),indent=2)+'\n')
