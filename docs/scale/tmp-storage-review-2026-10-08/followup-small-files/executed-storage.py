import sys, os, json, shutil, hashlib, datetime, importlib.util, subprocess
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf')
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('old','/tmp/cleanup-evidence-storage-20261008.py')
old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
base=repo/'docs/scale/tmp-storage-review-2026-10-08/followup-small-files'
stage=Path('/home/exedev/tmp-followup-staging')
archive=Path('/home/exedev/tmp-followup.tar.gz')
cutoff=datetime.datetime(2026,10,8,4,tzinfo=datetime.timezone.utc).timestamp()
def candidates():
 selected=[];skipped={}
 for p in sorted(Path('/tmp').iterdir()):
  if p.name.startswith(('.', 'systemd-', 'snap-', 'codex-', 'claude-', 'cc-daemon-', 'shelley-', 'tmp.')) or p.name in ('opencode','node-compile-cache','__pycache__'):continue
  if p.is_symlink():continue
  if p.is_file():
   if p.stat().st_mtime>=cutoff:continue
   members=[p]
  elif p.is_dir():members=[p,*p.rglob('*')]
  else:continue
  if any(q.name=='.git' or q.is_symlink() or not (q.is_file() or q.is_dir()) for q in members):
   skipped[str(p)]='Git metadata, symlink or special file';continue
  selected.append(p)
 return selected,skipped
if len(sys.argv)>1 and sys.argv[1]=='closure':
 roots=[Path(p) for p in json.loads((base/'capture.json').read_text())['roots']]
 print(json.dumps(old.closure(roots)))
elif len(sys.argv)>1 and sys.argv[1]=='capture':
 selected,skipped=candidates()
 # This subprocess runs as root so live references from other users cannot be missed.
 probe=Path('/home/exedev/tmp-followup-roots.json');probe.write_text(json.dumps([str(p) for p in selected]))
 base.mkdir(parents=True,exist_ok=False)
 code="import runpy,json;from pathlib import Path;m=runpy.run_path('/home/exedev/tmp-cleanup-followup.py',run_name='closure_module');print(json.dumps(m['old'].closure([Path(p) for p in json.load(open('/home/exedev/tmp-followup-roots.json'))])))"
 checks=json.loads(subprocess.check_output(['sudo','-n','python3','-c',code],text=True))
 assert not checks['permission_limits'],checks
 selected=[p for p in selected if str(p) not in checks['blocked']]
 assert selected and not stage.exists() and not archive.exists()
 stage.mkdir()
 for p in selected:
  shutil.copytree(p,stage/p.name,copy_function=shutil.copy2) if p.is_dir() else shutil.copy2(p,stage/p.name)
 proof=fixture_archive.capture(stage,archive,base,compresslevel=6)
 report=dict(roots=[str(p) for p in selected],skipped=skipped,closure_before=checks,cutoff_utc='2026-10-08T04:00:00Z',archive=str(archive),utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Storage preservation only. Older loose files and inactive regular-file fixtures. Active tool state, Git worktrees and metadata, attachment and newer loose files retained. No test verdict changes.')
 (base/'capture.json').write_text(json.dumps(report,indent=2)+'\n')
 shutil.copyfile(__file__,base/'executed-storage.py')
 shutil.rmtree(stage)
 print(json.dumps(dict(roots=len(selected),files=proof['members']-1,original_bytes=sum(v['bytes'] for v in json.loads((base/'fixture-inventory.json').read_text())['files'].values()),archive_bytes=proof['archive_bytes'],blocked=checks['blocked'])))
