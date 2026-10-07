import sys
sys.dont_write_bytecode=True
from pathlib import Path
import subprocess,json,datetime,shutil,hashlib
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-candidate-partition200-v2-20261007');root.mkdir();source=root/'source'
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
patterns='/*\n!/docs/scale/**\n/docs/scale/**/\n/docs/scale/**/*.go\n/docs/scale/lease-partition-component-2026-10-06/contiguous-component/**\n'
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=patterns.encode(),check=True)
subprocess.run(['git','read-tree','-mu','HEAD'],cwd=source,check=True)
assert subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
selected_go=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=source,text=True).splitlines()
for name in selected_go:
 assert (source/name).is_file() and not (source/name).is_symlink(),name
 assert hashlib.sha256((source/name).read_bytes()).hexdigest()==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest(),name
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
record={'source':revision,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'unit':unit,'root':str(root),'source_checkout':str(source),'command':command,'launcher_command':launch,'sparse_patterns':patterns,'all_selected_go_inputs_verified':len(selected_go),'candidate_sha256':'a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68','scope':'Full original200 partition seeds at exact experimental component profile; no default dependency or release qualification. Native10m/18m SDK/25m seed envelopes/count1 preserved. Stops at first failure, no retry.'}
(root/'launch.json').write_text(json.dumps(record,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-launcher.py')
print('CANDIDATE_PARTITION200_SUPERVISOR_STARTED',revision,flush=True)
