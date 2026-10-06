from pathlib import Path
import subprocess,json,hashlib,datetime,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006');watch=Path('/tmp/js-wf-bulk-journal-24h-joined-watch-20261006')
read=lambda p:json.loads(p.read_text())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
state=read(root/'execution.json');assert state['status']=='running' and state['row']=='journal' and state['duration']=='24h' and state['seed']==1 and state['race'] is False
assert state['chunked_state_retained_audit'] and state['bulk_final_latency'] and state['compare_bulk_point']
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-bulk-journal-24h-joined-20261006.service','-p','ActiveState','-p','MainPID','-p','MemoryMax','-p','CPUQuotaPerSecUSec'],text=True).splitlines());assert unit['ActiveState']=='active' and int(unit['MainPID'])==state['supervisor_pid']
parent=read(watch/'parent.json')['pid'];sdks=[json.loads(l) for l in (watch/'sdks.jsonl').read_text().splitlines()];actual=next(x for x in sdks if x['pid']==parent)
proc=Path('/proc')/str(parent);assert proc.exists() and (proc/'stat').read_text().rsplit(')',1)[1].split()[19]==actual['stat'].rsplit(')',1)[1].split()[19]
binary=read(root/'binary.json');assert sha(root/'integration.test')==sha(proc/'exe')==actual['sha256']==binary['sha256']
assert 'vcs.revision='+state['source'] in actual['build_info'] and 'vcs.modified=false' in actual['build_info']
expected=dict(GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT='1',WF_MATRIX_BULK_FINAL_LATENCY='1',WF_MATRIX_BULK_POINT_COMPARE='1')
env=dict(e.decode().split('=',1) for e in (proc/'environ').read_bytes().split(b'\0') if b'=' in e)
assert actual['profile_environment']=={k:env[k] for k in expected}==expected
before=read(root/'source-before.json');assert before['revision']==state['source'];assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
for name,h in before['files'].items():
 assert sha(root/'source'/name)==h
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',state['source']+':'+name],cwd=repo)).hexdigest()==h
servers=[json.loads(l) for l in (watch/'servers.jsonl').read_text().splitlines()]
assert {x['inspect']['Name'].rsplit('-n',1)[-1] for x in servers}=={'0','1','2','3','4'}
server_sha=sha(root/'fixture/cluster/nats-server')
for x in servers:assert x['sha256']==server_sha and '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in x['build_info']
events=[json.loads(l) for l in (root/'events.jsonl').read_text().splitlines()]
faults=[x['Output'].strip() for x in events if 'tier3 journal fault node=' in x.get('Output','')]
assert faults and not any(x['Action']=='fail' for x in events)
normal_unit=subprocess.check_output(['systemctl','--user','show','js-wf-tier1-handler-boundary-normal100k-20261006.service','-p','ActiveState','-p','MainPID'],text=True)
out=repo/'docs/scale/bulk-journal-24h-2026-10-06/launch';out.mkdir()
for role in ('watch','launch'):shutil.copyfile(Path('/tmp/js-wf-'+role+'-bulk-journal-24h-joined-20261006.py'),out/('executed-'+role+'.py'))
shutil.copyfile(__file__,out/'executed-launch-verifier.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),execution=state,live_unit=unit,actual_sdk=actual,source_files_exact_git_current_before=len(before['files']),server_observations=len(servers),observed_distinct_names=sorted({x['inspect']['Name'] for x in servers}),actual_server_sha256=server_sha,overlap_reserve_bytes=state['disk_admission']['additional_reserve_bytes'],minute_disk_samples=[json.loads(l) for l in (watch/'disk-usage.jsonl').read_text().splitlines()],confirmed_journal_faults_so_far=faults,overlapping_normal_qualifier=normal_unit,scope='Verified live fresh original24h journal-leader seed1 at recorded executed source with explicit chunked/bulk/point comparison. Actual SDK/source/profile and five observed NATS executables verified; shared CPU with full100k normal qualifier recorded. Original deadlines/counts/history/drain/p99/checkpoint/checkpoint gates unchanged. Terminal native/full artifact/source-after/observer/process-closure and complete proof remain mandatory; no row/default/fullmatrix/24h qualification.')
(out/'launch.json').write_text(json.dumps(report,indent=2)+'\n');print('LIVE_24H_JOURNAL_SDK_SOURCE_PROFILE_SERVERS_VERIFIED',parent,len(before['files']),len(servers),flush=True)
