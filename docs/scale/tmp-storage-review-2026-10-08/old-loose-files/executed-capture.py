import sys,json,datetime,shutil,os
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
base=repo/'docs/scale/tmp-storage-review-2026-10-08/old-loose-files';base.mkdir()
stage=Path('/tmp/js-wf-old-loose-storage-stage-20261008');stage.mkdir()
shutil.copyfile(__file__,base/'executed-capture.py')
cutoff=datetime.datetime(2026,10,8,tzinfo=datetime.timezone.utc).timestamp()
selected=[];skipped=[]
for p in sorted(Path('/tmp').iterdir()):
 if p.is_symlink() or not p.is_file():continue
 stat=p.stat()
 if stat.st_size<262144 or stat.st_mtime>=cutoff or p.suffix=='.py' or p.name.endswith(('.tar.gz','.tgz','.zip')):continue
 try:checks=closure(p)
 except AssertionError as e:
  skipped.append(dict(path=str(p),reason=repr(e)));continue
 if stat.st_nlink!=1:
  skipped.append(dict(path=str(p),reason='existing hardlinks'));continue
 os.link(p,stage/p.name)
 selected.append(dict(path=str(p),device=stat.st_dev,inode=stat.st_ino,closure=checks))
print('SELECTED',len(selected),'BYTES',sum(Path(x['path']).stat().st_size for x in selected),flush=True)
proof=fixture_archive.capture(stage,Path('/tmp/js-wf-old-loose-storage-complete-20261008.tar.gz'),base,compresslevel=1)
# These remain hardlinked to originals. Recheck all bytes, inode identity and closure.
for row in selected:
 p=Path(row['path']);assert p.stat().st_ino==row['inode'] and p.stat().st_dev==row['device'];closure(p)
(base/'capture.json').write_text(json.dumps(dict(selected=selected,skipped=skipped,stage=str(stage),archive='/tmp/js-wf-old-loose-storage-complete-20261008.tar.gz',proof=proof,scope='Complete closed loose files of at least256KiB last modified before2026-10-08. Scripts, live/open files, symlinks, existing hardlinks and archive copies excluded. Storage-only preservation, no acceptance inference.'),indent=2)+'\n')
print('ARCHIVED',proof['archive_bytes'],flush=True)
