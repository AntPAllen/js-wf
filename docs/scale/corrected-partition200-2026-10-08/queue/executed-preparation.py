import sys,json,subprocess,datetime,shutil,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-corrected-partition200-20261008');source=root/'source'
assert not root.exists() and not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert revision==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert shutil.disk_usage('/tmp').free>=20*1024**3
root.mkdir();shutil.copy2(__file__,root/'executed-preparation.py')
subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
patterns='/*\n!/docs/scale/*\n/docs/scale/lease-partition-component-2026-10-06/\n!/docs/scale/lease-partition-component-2026-10-06/*\n/docs/scale/lease-partition-component-2026-10-06/contiguous-component/\n'
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=patterns,text=True,check=True)
subprocess.run(['git','read-tree','-mu','HEAD'],cwd=source,check=True)
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
proof='docs/scale/lease-partition-component-2026-10-06/contiguous-component'
for name in ['archive-verification.json','fixture-inventory.json','s3-readback.json','candidate-build.json','independent-review.json']:
 assert (source/proof/name).read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+proof+'/'+name],cwd=repo)
program=Path('/tmp/js-wf-candidate-partition200-input-20261007/component/candidate-server')
with program.open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
assert digest=='a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68'
gatebase=repo/'docs/scale/tier1-full126-2026-10-08/race-launch';gate=json.loads((gatebase/'launch.json').read_text())
assert (gatebase/'launch.json').read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':'+str((gatebase/'launch.json').relative_to(repo))],cwd=repo)
sys.path.insert(0,str(source/'scripts'));from live_process_admission import snapshot
pid=gate['execution']['actual_sdk']['pid'];profile=gate['execution']['actual_sdk']['environment'];first=snapshot(pid,profile)
with Path('/proc',str(pid),'exe').open('rb') as f:actual=hashlib.file_digest(f,'sha256').hexdigest()
second=snapshot(pid,profile)
keys=['pid','start_ticks','args','exe','working_directory','environment']
assert {k:first[k] for k in keys}=={k:second[k] for k in keys} and actual==gate['actual_sdk_sha256'] and second['start_ticks']==gate['execution']['actual_sdk']['start_ticks']
command=['/usr/bin/python3',str(source/'scripts/run-local-tier2-partition-campaign.py'),'--root',str(root/'campaign'),'--minimum-free-bytes',str(20*1024**3),'--partition-server',str(program),'--partition-server-proof',proof]
record=dict(source=revision,prepared_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),source_checkout=str(source),sparse_patterns=patterns,command=command,candidate_sha256=digest,resource_gate=dict(unit='js-wf-tier1-full126-race-20261008.service',invocation_id=gate['unit']['InvocationID'],sdk=second,exe_sha256=actual),status='prepared_waiting_for_original_race_handle',native_started=False,original_seeds=list(range(1,201)),duration='10m',sdk_timeout='18m',seed_job_envelope='25m',race=False,scope='Fresh corrected-harness candidate200 preparation, no native execution or qualification. Old failed seed31/campaign unchanged. Wait for the admitted full126 SDK and original supervisor to close; do not restart either qualification.')
(root/'preparation.json').write_text(json.dumps(record,indent=2)+'\n');print('PREPARED_CORRECTED_FULL200',revision,pid,flush=True)
