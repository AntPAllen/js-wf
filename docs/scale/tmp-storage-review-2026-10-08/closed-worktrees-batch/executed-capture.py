import sys,json,subprocess,importlib.util,shutil,datetime,hashlib,os
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
spec=importlib.util.spec_from_file_location('old','/tmp/cleanup-evidence-storage-20261008.py');old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
base=repo/'docs/scale/tmp-storage-review-2026-10-08/closed-worktrees-batch'
archive=Path('/tmp/tmp-closed-worktrees-20261008.tar.gz')
stage=Path('/tmp/tmp-closed-worktrees-stage-20261008')
roots=[Path('/tmp')/n for n in ('js-wf-tier1-full126-normal100k-20261008','js-wf-local-partition200-source-20261006','js-wf-hosted-partition-seed3-review-checkout-20261005','js-wf-audit-reduction-full-population-profile-20261005','js-wf-partition-review-positive-source-20261006','js-wf-planner-checkout-ac3cd53')]
def worktrees():
    result=[]
    for block in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).split('\n\n'):
        lines=block.splitlines()
        if not lines:continue
        path=Path(lines[0].removeprefix('worktree '))
        if any(path==r or path.is_relative_to(r) for r in roots):
            status=subprocess.check_output(['git','-C',str(path),'status','--porcelain'],text=True)
            assert not status,(path,status)
            result.append(dict(path=str(path),head=lines[1].removeprefix('HEAD '),status=status))
    return result
if len(sys.argv)>1 and sys.argv[1]=='closure':
    print(json.dumps(old.closure(roots+[archive])));sys.exit()
assert not base.exists() and not stage.exists() and not archive.exists()
wt=worktrees();assert len(wt)==len(roots)
closure=json.loads(subprocess.check_output(['sudo','-n','python',__file__,'closure'],text=True))
assert not closure['blocked'] and not closure['permission_limits'],closure
base.mkdir(parents=True);stage.mkdir()
for r in roots:shutil.copytree(r,stage/r.name,copy_function=shutil.copy2)
proof=fixture_archive.capture(stage,archive,base,compresslevel=1)
inv=json.loads((base/'fixture-inventory.json').read_text())
for r in roots:assert fixture_archive.inventory(r)=={k[len(r.name)+1:]:v for k,v in inv['files'].items() if k.startswith(r.name+'/')}
assert wt==worktrees()
report=dict(roots=list(map(str,roots)),worktrees=wt,privileged_closure=closure,archive=str(archive),utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Closed fixture preservation; full regular-file bytes including source and .git pointer files. Git commits remain in the main repository. No verdict changes. Worktree registrations will be removed only after committed S3 verification.')
(base/'capture.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-capture.py')
(base/'README.md').write_text('# Closed source checkouts and simulation fixture\n\nComplete archives of six closed roots, including the accepted frozen full126 normal100k campaign. The archive preserves original bytes, modes and mtimes; the inventory records every file. Restored source is standalone; archived `.git` pointer files refer to retired local worktree registrations. Campaign verdicts and remaining implementation requirements are unchanged.\n')
shutil.rmtree(stage)
print(json.dumps(dict(roots=len(roots),files=len(inv['files']),archive_bytes=proof['archive_bytes'])))
