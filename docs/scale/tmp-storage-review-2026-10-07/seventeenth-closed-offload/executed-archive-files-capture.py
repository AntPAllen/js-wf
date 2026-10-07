import sys,json,subprocess,datetime,shutil
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
base=repo/'docs/scale/tmp-storage-review-2026-10-07/seventeenth-closed-offload'
report=json.loads((base/'selection.json').read_text())
# Preserve closed top-level files separately; hardlinks avoid duplicating large binaries.
stage=Path('/tmp/seventeenth-closed-archive-files-20261007')
originals=[]
for original in Path('/tmp').iterdir():
 if original.is_symlink() or not original.is_file() or original.stat().st_size<1*1024*1024:continue
 if original.name.startswith('seventeenth-') or not original.name.endswith(('.tar.gz','.tgz')):continue
 try:
  assert original.stat().st_nlink==1
  c=closure(original)
 except Exception as e:
  report['retained'].append({'root':str(original),'bytes':original.stat().st_size,'reason':repr(e)[:300]});continue
 stage.mkdir(exist_ok=True)
 import os
 st=original.stat();os.link(original,stage/original.name)
 originals.append({'path':str(original),'device':st.st_dev,'inode':st.st_ino,'bytes':st.st_size,'closure':c})
if originals:
 out=base/stage.name;out.mkdir()
 archive=Path('/tmp')/('seventeenth-'+stage.name+'.tar.gz')
 (out/'closure-before.json').write_text(json.dumps(closure(stage),indent=2)+'\n')
 result=fixture_archive.capture(stage,archive,out,compresslevel=1)
 (out/'closure-after.json').write_text(json.dumps(closure(stage),indent=2)+'\n')
 origin={'root':str(stage),'archive':str(archive),'qualification':'Closed standalone file preservation only; hardlink staging. Original verdicts unchanged.','original_files':originals}
 (out/'origin.json').write_text(json.dumps(origin,indent=2)+'\n')
 report['selected'].append(origin)
 print('CAPTURED_TOP_FILES',len(originals),result['archive_bytes'],flush=True)
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
(base/'selection.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-archive-files-capture.py')
