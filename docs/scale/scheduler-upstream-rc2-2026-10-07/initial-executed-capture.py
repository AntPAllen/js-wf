import sys,json,subprocess,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import closure,fixture_archive,repo
root=Path('/tmp/js-wf-upstream-scheduler-rc2-20261007');base=repo/'docs/scale/scheduler-upstream-rc2-2026-10-07'
unit='js-wf-upstream-scheduler-rc2-20261007.service'
raw=subprocess.check_output(['systemctl','show',unit,'-p','LoadState','-p','SubState','-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID','-p','Restart'],text=True)
fields=dict(line.split('=',1) for line in raw.splitlines());assert fields['LoadState']=='loaded' and fields['SubState']=='exited' and fields['MainPID']=='0' and fields['ExecMainStatus']=='0' and fields['Result']=='success' and fields['Restart']=='no'
assert json.loads((root/'source.json').read_text())['clean'] is True
(root/'native-terminal-unit.txt').write_text(raw)
(root/'native-journal.log').write_bytes(subprocess.check_output(['journalctl','-u',unit,'--no-pager','-o','short-iso']))
release=json.loads(subprocess.check_output(['gh','api','repos/nats-io/nats-server/releases/tags/v2.15.1-RC.2']))
release={k:release[k] for k in ('tag_name','published_at','html_url','prerelease','target_commitish')}
tag=json.loads(subprocess.check_output(['gh','api','repos/nats-io/nats-server/git/ref/tags/v2.15.1-RC.2']))
source=json.loads((root/'source.json').read_text());assert tag['object']['type']=='commit' and tag['object']['sha']==source['download']['Origin']['Hash']
(root/'official-release-metadata.json').write_text(json.dumps(release,indent=2)+'\n');(root/'official-tag.json').write_text(json.dumps(tag,indent=2)+'\n')
for src,name in [('/tmp/scheduler-rc2-independent-review-20261007.json','independent-review.json'),('/tmp/scheduler-rc2-provenance-controls-20261007.json','provenance-controls.json'),('/tmp/scheduler-rc2-compat-stable-restore-20261007.json','stable-restore.json'),('/tmp/scheduler-rc2-compat-prior-restore-20261007.json','prior-restore.json'),('scripts/review-nats-scheduler-cleanup.py','executed-review.py'),('scripts/test_scheduler_upstream_review.py','executed-provenance-controls.py'),('/tmp/restore_scheduler_compat.py','executed-compatibility-restore.py')]:shutil.copyfile(repo/src if not src.startswith('/') else src,root/name)
(root/'initial-compatibility-error.json').write_text(json.dumps({'error':'FileNotFoundError: /tmp/js-wf-scheduler-complete-originals-20261003/baseline/compiled-inventory.json','phase':'initial read-only compatibility attempt used already-trimmed legacy local root; failed before provenance controls','resolution':'Fresh complete stable and RC1 S3 restores verified before unchanged native evidence was reviewed; no native rerun/store reopen.'},indent=2)+'\n')
before=closure(root);a=fixture_archive.capture(root,Path('/tmp/js-wf-upstream-scheduler-rc2-complete-20261007.tar.gz'),base,compresslevel=1);after=closure(root)
(base/'capture.json').write_text(json.dumps({'native_terminal_unit':raw,'closure_before':before,'closure_after':after,'archive':a,'scope':'Complete RC2 fresh two-message baseline/control evidence only; no server/Raft/original-million store or full drain qualification.'},indent=2)+'\n')
for p in root.glob('*.json'):shutil.copyfile(p,base/p.name)
for p in root.glob('*.py'):shutil.copyfile(p,base/p.name)
for p in root.glob('*.txt'):shutil.copyfile(p,base/p.name)
for p in root.glob('*.log'):shutil.copyfile(p,base/p.name)
for phase in ['baseline','dirty-control']:
 dest=base/phase;dest.mkdir()
 for p in (root/phase).iterdir():
  if p.is_file():shutil.copyfile(p,dest/(p.name+'.txt' if p.suffix=='.go' else p.name))
shutil.copyfile(__file__,base/'executed-capture.py')
print(json.dumps(a))
