from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
root=Path('/tmp/js-wf-upstream-scheduler-rc1-20261006');base=repo/'docs/scale/scheduler-upstream-rc1-2026-10-06'
base.mkdir(exist_ok=True)
ns={};text=Path('/tmp/js-wf-offload-historical-matrix-evidence-20261006.py').read_text();exec(text[:text.index('names=')],ns)
initial=ns['closure'](root)
unit='js-wf-upstream-scheduler-rc1-20261006.service'
raw=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStatus','-p','InvocationID','-p','ExecStart','-p','MemoryMax','-p','CPUQuotaPerSecUSec'],text=True)
assert 'ActiveState=inactive' in raw and 'MainPID=0' in raw and 'ExecMainStatus=0' in raw
(root/'native-terminal-unit.txt').write_text(raw)
(root/'native-journal.log').write_bytes(subprocess.check_output(['journalctl','--user','-u',unit,'--no-pager','-o','short-iso']))
for name in ['review-nats-scheduler-cleanup.py','test_scheduler_upstream_review.py']:
 shutil.copyfile(repo/'scripts'/name,root/name)
shutil.copyfile('/tmp/js-wf-upstream-scheduler-review-controls-20261006.json',root/'review-controls.json')
shutil.copyfile('/tmp/js-wf-stable-cleanup-compatibility-20261006.json',root/'stable-compatibility.json')
subprocess.run(['python3',str(root/'review-nats-scheduler-cleanup.py'),str(root),'--repo',str(repo),'--expected-version','v2.15.1-RC.1','--require-retained-store'],check=True,stdout=subprocess.DEVNULL)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
(root/'reviewer-source.json').write_text(json.dumps(dict(revision=revision,files={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ['review-nats-scheduler-cleanup.py','test_scheduler_upstream_review.py']}),indent=2)+'\n')
proof=fixture_archive.capture(root,Path('/tmp/js-wf-upstream-scheduler-rc1-complete-20261006.tar.gz'),base,compresslevel=1)
final=ns['closure'](root)
(base/'capture.json').write_text(json.dumps(dict(closure_before=initial,closure_after=final,complete_archive=proof,native_terminal_unit=raw,scope='Complete fresh two-message RC baseline and causal control evidence only; no original million-store mutation or server/Raft/full drain qualification.'),indent=2)+'\n')
shutil.copyfile(__file__,base/'executed-capture.py')
print(json.dumps(dict(archive_bytes=proof['archive_bytes'],archive_sha256=proof['archive_sha256'])))
