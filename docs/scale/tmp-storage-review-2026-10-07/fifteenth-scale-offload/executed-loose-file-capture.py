import sys,json,os
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive
base=Path('/home/exedev/js-wf/docs/scale/tmp-storage-review-2026-10-07/fifteenth-scale-offload/top-level-files')
stage=Path('/tmp/closed-topfiles-stage-20261007');stage.mkdir()
checks={};skipped={}
for p in sorted(Path('/tmp').iterdir()):
 if not p.is_file() or p.is_symlink() or p.stat().st_size<15*1024*1024 or p.name.startswith('remaining-'):continue
 if p.suffix=='.gz':continue
 try:c=closure(p)
 except Exception as e:skipped[str(p)]=repr(e);continue
 os.link(p,stage/p.name);checks[p.name]=c
base.mkdir(parents=True)
(base/'origins-and-closure.json').write_text(json.dumps({'original_parent':'/tmp','staging':'Hardlinks to closed regular files only; original files remain until complete S3 readback.','selected':checks,'skipped':skipped},indent=2)+'\n')
result=fixture_archive.capture(stage,Path('/tmp/remaining-top-level-files-20261007.tar.gz'),base,compresslevel=1)
print(json.dumps({'files':len(checks),'selected_bytes':sum(p.stat().st_size for p in stage.iterdir()),'skipped':skipped,'archive':result}),flush=True)
