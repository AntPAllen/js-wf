from pathlib import Path
import json,hashlib,subprocess,shutil,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-held-metadata-rollout-10m-20261005');out=Path('/tmp/js-wf-held-metadata-rollout-10m-launch-proof-20261005');out.mkdir()
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
e=json.loads((r/'execution.json').read_text());b=json.loads((r/'binary.json').read_text());before=json.loads((r/'source-before.json').read_text());assert e['status']=='running' and e['source']==before['revision'] and sha(r/'integration.test')==b['sha256']
lines=Path('/tmp/js-wf-held-metadata-rollout-10m-live-20261005/sdks.jsonl').read_text().splitlines();sdks=[json.loads(line) for line in lines];parent=[p for p in sdks if 'TestFiveContainerMixedWorkerKilledEveryFiveSeconds' in p['args'][1]];assert len(parent)==1
assert sha(Path(f"/proc/{parent[0]['pid']}/exe"))==b['sha256']
for captured in sdks:
 assert captured['sha256']==b['sha256'] and captured['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
for n,d in before['files'].items():
 assert sha(r/'source'/n)==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d
 p=out/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(r/'source'/n,p)
ids=subprocess.check_output(['docker','ps','--filter',f"name=js-wf-route-{parent[0]['pid']}-",'--format','{{.ID}}'],text=True).splitlines();assert len(ids)==5
containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True));containers.sort(key=lambda c:int(c['Name'].rsplit('-n',1)[1]));proof=[]
server=r/'fixture/cluster/nats-server';server_hash=sha(server);server_info=subprocess.check_output(['go','version','-m',str(server)],text=True).splitlines()[1:]
for i,c in enumerate(containers):
 pid=c['State']['Pid'];assert c['State']['Running'] and pid>0
 assert c['Name'].endswith(f'-n{i}')
 assert any(m['Source']==str(r/'fixture/cluster'/f'node-{i}') and m['Destination']=='/data' for m in c['Mounts'])
 live=f'/proc/{pid}/exe';digest=subprocess.check_output(['sudo','-n','sha256sum',live],text=True).split()[0];assert digest==server_hash
 info=subprocess.check_output(['sudo','-n','/usr/local/go/bin/go','version','-m',live],text=True);assert info.splitlines()[1:]==server_info
 p=out/f'node-{i}-nats-server'
 with p.open('wb') as f:subprocess.run(['sudo','-n','cat',live],stdout=f,check=True)
 assert sha(p)==digest
 proof.append({'node':i,'container_id':c['Id'],'name':c['Name'],'pid':pid,'executable_sha256':digest,'build_info':info,'path':c['Path'],'args':c['Args'],'mounts':c['Mounts']})
for n in ['execution.json','commands.json','binary.json','source-before.json','test-environment.json','integration.test']:shutil.copyfile(r/n,out/n)
(out/'live-sdk-snapshot.json').write_text(json.dumps(sdks,indent=2)+'\n');(out/'actual-servers.json').write_text(json.dumps(proof,indent=2)+'\n')
review={'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':e['source'],'actual_parent_pid':parent[0]['pid'],'actual_sdk_sha256':b['sha256'],'captured_sdk_processes_at_snapshot':len(sdks),'actual_live_server_processes':5,'actual_server_sha256':server_hash,'source_git_before_inputs':len(before['files']),'all_actual_sdk_and_server_hashes_build_fields_verified':True,'source_inputs_match_recorded_git':True,'journal_rollout':'protobuf-to-json','duration':'10m','live_store_snapshot':False,'terminal':False,'qualifies_sustained_row':False,'qualifies_full_matrix':False,'qualifies_24h':False}
(out/'review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-capture.py');print(json.dumps(review))
