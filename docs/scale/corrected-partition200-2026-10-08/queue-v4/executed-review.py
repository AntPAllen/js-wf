import sys,json,hashlib,subprocess,datetime,shutil,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-corrected-partition200-20261008');source=root/'source';out=repo/'docs/scale/corrected-partition200-2026-10-08/queue-v4'
record=json.loads((root/'preparation.json').read_text());state=json.loads((root/'resource-gate.json').read_text());rev=record['source']
assert rev=='2ef3e8bea19c233f14c12564b4941b721913ba6b'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==rev and not subprocess.check_output(['git','status','--porcelain'],cwd=source)
assert record['original_seeds']==list(range(1,201)) and record['duration']=='10m' and record['sdk_timeout']=='18m' and record['seed_job_envelope']=='25m' and record['race'] is False
assert record['command']==['/usr/bin/python3',str(source/'scripts/run-local-tier2-partition-campaign.py'),'--root',str(root/'campaign'),'--minimum-free-bytes',str(20*1024**3),'--partition-server','/tmp/js-wf-candidate-partition200-input-20261007/component/candidate-server','--partition-server-proof','docs/scale/lease-partition-component-2026-10-06/contiguous-component']
assert state['status']=='waiting_for_original_race_handle' and state['native_started'] is False and state['original_sdk_live'] and not (root/'campaign').exists()
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show','js-wf-corrected-partition200-v4-20261008.service','--property=MainPID,ExecMainPID,InvocationID,ActiveState,SubState,Result,Type,Restart,RuntimeMaxUSec,MemoryMax,Nice,CPUWeight,ExecStart'],text=True).splitlines())
assert unit['MainPID']==str(state['supervisor_pid']) and unit['ActiveState']=='active' and unit['SubState']=='running' and unit['Type']=='exec' and unit['Restart']=='no' and unit['RuntimeMaxUSec']=='3d 18h' and unit['MemoryMax']=='4294967296'
assert unit['Nice']=='19' and unit['CPUWeight']=='5'
assert '/tmp/watch_start_corrected_partition200_v4_20261008.py' in unit['ExecStart']
sys.path.insert(0,str(source/'scripts'));from live_process_admission import snapshot
profile={'PYTHONDONTWRITEBYTECODE':'1'};first=snapshot(state['supervisor_pid'],profile);second=snapshot(state['supervisor_pid'],profile)
assert first['start_ticks']==second['start_ticks']==state['supervisor_start_ticks'] and second['args']==['/usr/bin/python3','/tmp/watch_start_corrected_partition200_v4_20261008.py']
original=record['resource_gate']['sdk'];a=snapshot(original['pid'],original['environment'])
with Path('/proc',str(original['pid']),'exe').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
b=snapshot(original['pid'],original['environment']);keys=['pid','start_ticks','args','exe','working_directory','environment']
assert {k:a[k] for k in keys}=={k:b[k] for k in keys}=={k:original[k] for k in keys}
assert digest==record['resource_gate']['exe_sha256']
originalunit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',record['resource_gate']['unit'],'--property=MainPID,InvocationID,ActiveState,SubState'],text=True).splitlines())
assert originalunit['InvocationID']==record['resource_gate']['invocation_id'] and int(originalunit['MainPID'])>0
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines()
selected=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum') or n.startswith('scripts/') or n=='.github/workflows/tier2-retained-row.yml' or n.startswith('docs/scale/lease-partition-component-2026-10-06/contiguous-component/')]
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in selected).encode(),cwd=repo))
inputs={}
for n in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert (source/n).read_bytes()==data;inputs[n]=hashlib.sha256(data).hexdigest()
assert not stream.read()
with Path(record['command'][record['command'].index('--partition-server')+1]).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==record['candidate_sha256']=='a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68'
assert (root/'executed-resource-gate.py').read_bytes()==Path('/tmp/watch_start_corrected_partition200_v4_20261008.py').read_bytes()
out.mkdir(parents=True)
for n in ['preparation.json','resource-gate.json','executed-preparation.py','executed-resource-gate.py']:shutil.copy2(root/n,out/n)
shutil.copytree(root/'initial-queue-supervision',out/'initial-queue-supervision')
shutil.copytree(root/'second-queue-supervision',out/'second-queue-supervision')
shutil.copytree(root/'third-queue-supervision',out/'third-queue-supervision')
shutil.copy2(__file__,out/'executed-review.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=rev,selected_inputs=inputs,unit=unit,actual_supervisor=second,original_race_sdk=b,original_race_exe_sha256=digest,original_race_unit=originalunit,native_started=False,scope='Verified live resource-gated corrected full200 preparation at recorded source/candidate; original SDK and supervisor identities remain live. No compilation/native workload/first-seed coverage/terminal acceptance. Initial queue-only supervisor superseded before any campaign producer or SDK to add actual SDK admission; no native restart.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('CORRECTED_FULL200_QUEUE_INDEPENDENTLY_REVIEWED',state['supervisor_pid'],original['pid'],len(inputs),flush=True)
