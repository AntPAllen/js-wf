import sys
sys.dont_write_bytecode=True
from pathlib import Path
import subprocess,json,datetime,shutil,hashlib
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-candidate-partition200-20261007');source=root/'source'
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()
assert revision.startswith('f3f7913') and not (root/'launch.json').exists()
assert subprocess.check_output(['systemctl','--user','show',root.name+'.service','--property=LoadState','--value'],text=True).strip()=='not-found'
assert subprocess.check_output(['git','ls-files'],cwd=source)==b'' and sorted(p.name for p in source.iterdir())==['.git']
shutil.copyfile('/tmp/launch_candidate_partition200_initial.py',root/'initial-executed-launcher.py')
(root/'initial-preparation-rejection.json').write_text(json.dumps({'rejection':'Sparse set did not populate the empty index of the --no-checkout worktree; clean-source assertion rejected before any supervisor or SDK existed.','native_started':False,'resume':'Populate the same empty selected worktree using git read-tree -mu HEAD; no native restart.'},indent=2)+'\n')
subprocess.run(['git','read-tree','-mu','HEAD'],cwd=source,check=True)
patterns=subprocess.check_output(['git','sparse-checkout','list'],cwd=source,text=True)
assert subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==revision
proof='docs/scale/lease-partition-component-2026-10-06/contiguous-component'
for name in ['archive-verification.json','fixture-inventory.json','s3-readback.json','candidate-build.json','independent-review.json']:
 assert (source/proof/name).read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+proof+'/'+name],cwd=repo)
program='/tmp/js-wf-candidate-partition200-input-20261007/component/candidate-server'
with open(program,'rb') as file:assert hashlib.file_digest(file,'sha256').hexdigest()=='a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68'
command=['/usr/bin/python3',str(source/'scripts/run-local-tier2-partition-campaign.py'),'--root',str(root/'campaign'),'--minimum-free-bytes',str(20*1024**3),'--partition-server',program,'--partition-server-proof',proof]
unit=root.name+'.service'
launch=['systemd-run','--user','--unit='+unit,'--property=RemainAfterExit=yes','--property=Restart=no','--property=Nice=19','--property=CPUWeight=5','--property=MemoryMax=4G','--property=RuntimeMaxSec=90h','--property=WorkingDirectory='+str(source),'--setenv=PYTHONDONTWRITEBYTECODE=1',*command]
subprocess.run(launch,check=True)
record={'source':revision,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'unit':unit,'root':str(root),'source_checkout':str(source),'command':command,'launcher_command':launch,'sparse_patterns':patterns,'candidate_sha256':'a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68','scope':'Full original200 partition seeds at exact experimental component profile; no default dependency or release qualification. Native10m/18m SDK/25m seed envelopes/count1 preserved. Stops at first failure, no retry.'}
(root/'launch.json').write_text(json.dumps(record,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-launcher.py')
print('CANDIDATE_PARTITION200_SUPERVISOR_STARTED',revision,flush=True)
