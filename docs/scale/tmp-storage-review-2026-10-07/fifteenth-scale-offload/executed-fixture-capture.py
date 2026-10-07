import sys,json,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
base=Path('/home/exedev/js-wf/docs/scale/tmp-storage-review-2026-10-07/fifteenth-scale-offload')
items=[('million-capacity','/tmp/wf-scale-1m'),('blockdisk-smoke','/tmp/js-wf-tier2-retained-blockdisk-smoke-20261005/originals/TestMixedMatrixBlockDiskStallEveryThirtySeconds/wf-block-1621114388'),('blockdisk-ten-minute','/tmp/js-wf-tier2-retained-blockdisk-ten-minute-20261005/originals/TestMixedMatrixBlockDiskStallEveryThirtySeconds/wf-block-2202913638')]
for label,name in items:
 root=Path(name);out=base/label
 c=closure(root)
 out.mkdir(parents=True,exist_ok=True)
 (out/'closure-before.json').write_text(json.dumps(c,indent=2)+'\n')
 result=fixture_archive.capture(root,Path('/tmp')/('remaining-'+label+'-20261007.tar.gz'),out,compresslevel=1)
 (out/'closure-after.json').write_text(json.dumps(closure(root),indent=2)+'\n')
 (out/'origin.json').write_text(json.dumps({'root':name,'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'qualification':'Storage preservation only; original producer/test verdict unchanged. Block media directory restored before reuse; no executable started.'},indent=2)+'\n')
 print(label,json.dumps(result),flush=True)
