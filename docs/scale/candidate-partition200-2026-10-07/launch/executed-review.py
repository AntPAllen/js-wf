import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,subprocess,hashlib,shutil,datetime,io
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import live_process_admission
root=Path('/tmp/js-wf-candidate-partition200-v2-20261007');source=root/'source';seed=root/'campaign/seed-001';out=repo/'docs/scale/candidate-partition200-2026-10-07/launch';out.mkdir()
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
launch=read(root/'launch.json');state=read(root/'campaign/campaign.json');execution=read(seed/'execution.json');rev=launch['source']
assert state['source']==execution['source']==rev and state['seeds']==list(range(1,201)) and state['server_profile']=='experimental-component-candidate' and state['records']==[] and state['current_seed']==1
assert state['native_coverage_complete'] is False and state['qualifies_full_row'] is False
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==rev and subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',root.name+'.service','--property=ActiveState,SubState,MainPID,ExecMainPID,InvocationID,Restart,MemoryMax,RuntimeMaxUSec,Nice,CPUWeight'],text=True).splitlines())
assert unit['ActiveState']=='active' and unit['SubState']=='running' and unit['MainPID']==str(state['producer_pid']) and unit['InvocationID']=='739815ede74f4562aca63e5a0fc52ee9' and unit['Restart']=='no'
def observe(pid,profile,expected):
 a=live_process_admission.snapshot(pid,profile)
 with Path('/proc',str(pid),'exe').open('rb') as f:h=hashlib.file_digest(f,'sha256').hexdigest()
 b=live_process_admission.snapshot(pid,profile)
 keys=['pid','start_ticks','args','exe','working_directory','environment']
 assert {k:a[k] for k in keys}=={k:b[k] for k in keys} and h==expected and Path('/proc',str(pid)).exists()
 assert b['environment']==profile
 return {'before':a,'after':b,'exe_sha256':h,'stable_actual_identity_observed_twice':True}
actual=observe(execution['pid'],execution['environment'],execution['sha256'])
assert actual['after']['start_ticks']==execution['start_ticks'] and actual['after']['args']==execution['actual_argv'] and actual['after']['working_directory']==str(source/'integration') and actual['after']['exe']==str(seed/'integration.test')
assert execution['duration']=='10m' and execution['race'] is False and actual['after']['args'][-1]=='-test.timeout=18m' and '-test.count=1' in actual['after']['args']
assert execution['environment']['GOMAXPROCS']=='2' and execution['environment']['GOMEMLIMIT']=='2GiB' and execution['environment']['FAULT_SEED']=='1'
assert '-race=true' not in execution['build_info'] and 'vcs.modified=false' in execution['build_info'] and 'vcs.revision='+rev in execution['build_info'] and sha(seed/'integration.test')==execution['sha256']
before=read(seed/'source-before.json');names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum')];assert set(selected)==set(before['files']) and before['revision']==rev
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in selected).encode()))
for name in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==before['files'][name]==sha(source/name)==sha(seed/'source'/name)
assert not stream.read()
external=read(seed/'external-source-before.json');captured=read(seed/'external-captured-paths.json')
for path,h in external.items():assert sha(Path(path))==h==sha(seed/captured[path])
servers=read(seed/'observed-servers.json');assert len(servers)==3 and len({s['pid'] for s in servers})==3
expected='a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68';peers=[]
for server in servers:
 assert server['actual_executable_sha256']==expected and server['argv'][0]==str(seed/'partition-server.bin')
 observed=observe(server['pid'],{'GOMAXPROCS':'2','GOMEMLIMIT':'2GiB'},expected)
 assert observed['after']['start_ticks']==server['start_ticks'] and observed['after']['args']==server['argv'] and observed['after']['stat'].rsplit(') ',1)[1].split()[1]==str(execution['pid'])
 peers.append(observed)
assert read(seed/'partition-server-input.json')['sha256']==state['candidate_sha256']==expected
report={'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':rev,'original_unit':unit,'actual_sdk':actual,'actual_three_candidate_servers':peers,'all_selected_go_inputs_verified':len(selected),'all_selected_external_inputs_verified':len(external),'seed':1,'original_seeds':list(range(1,201)),'scope':'Independent actual live first-seed admission only. All200 native bodies and independent terminal fault/history/store/source/archive qualification remain pending; no default-server or release qualification.'}
(out/'independent-launch-review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-review.py')
for p,name in [(root/'launch.json','launch.json'),(root/'executed-launcher.py','executed-launcher.py'),(root/'campaign/campaign.json','campaign-at-launch.json'),(seed/'execution.json','first-seed-execution-at-launch.json'),(seed/'commands.json','first-seed-commands.json'),(seed/'source-before.json','first-seed-source-before.json'),(seed/'external-source-before.json','first-seed-external-source-before.json'),(seed/'partition-server-input.json','first-seed-partition-server-input.json')]:shutil.copyfile(p,out/name)
restore=Path('/tmp/js-wf-candidate-partition200-input-20261007')
for name in ['restore.json','executed-restore.py']:shutil.copyfile(restore/name,out/name)
print('CANDIDATE_PARTITION200_FIRST_SDK_AND_THREE_PEERS_ADMITTED',execution['pid'],len(selected),len(external),flush=True)
