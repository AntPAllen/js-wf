import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,subprocess,shutil
sys.path.insert(0,'/tmp');from storage_review_common import repo,closure,fixture_archive
root=Path('/tmp/js-wf-candidate-partition200-20261007');source=root/'source';out=repo/'docs/scale/candidate-partition200-2026-10-07/preparation-failure';out.mkdir(parents=True)
state=json.loads((root/'campaign/campaign.json').read_text());assert state['status']=='preparation_failed' and state['records']==[] and state['exit_code']==1
assert not (root/'campaign/prepared/integration.test').exists() and not (root/'campaign/seed-001').exists()
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',root.name+'.service','--property=ActiveState,SubState,MainPID,ExecMainPID,ExecMainStatus,Result,InvocationID,Restart'],text=True).splitlines())
assert unit['MainPID']=='0' and unit['Result']=='exit-code' and unit['ExecMainStatus']=='1' and unit['InvocationID']=='4f646c41d9e6424ca4098efd1ff88478'
assert state['source']==subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip() and subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
assert 'FileNotFoundError' in (root/'campaign/prepare.log').read_text() and 'cas-throughput-placement-ci-2026-09-30/follower/main.go' in (root/'campaign/prepare.log').read_text()
report={'source':state['source'],'terminal_unit':unit,'native_started':False,'completed_seeds':0,'failure':'Inherited sparse source selection omitted a tracked Go fixture; original source checker rejected before compile/native launch.','fresh_closure':closure(root)}
(out/'failed-preparation-review.json').write_text(json.dumps(report,indent=2)+'\n')
for name in ['launch.json','executed-launcher.py','initial-executed-launcher.py','initial-preparation-rejection.json']:shutil.copyfile(root/name,out/name)
shutil.copyfile(root/'campaign/campaign.json',out/'campaign.json');shutil.copyfile(root/'campaign/prepare.log',out/'prepare.log');shutil.copyfile(__file__,out/'executed-preserve.py')
fixture_archive.capture(root,root.with_suffix('.tar.gz'),out,compresslevel=1)
print('FAILED_PREPARATION_PRESERVED_NO_NATIVE',flush=True)
