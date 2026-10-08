import sys,json,hashlib,subprocess,shutil,datetime,importlib.util,io,math
from pathlib import Path
sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp')
from storage_review_common import *
sys.path.insert(0,str(repo/'scripts'))
spec=importlib.util.spec_from_file_location('raw',repo/'scripts/check-tier2-journal-shard.py');raw=importlib.util.module_from_spec(spec);spec.loader.exec_module(raw)
spec=importlib.util.spec_from_file_location('review',repo/'scripts/review-local-tier2-partition.py');review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)
root=Path('/tmp/js-wf-corrected-partition200-20261008');campaign=root/'campaign';seed=campaign/'seed-002';out=repo/'docs/scale/corrected-partition200-2026-10-08/terminal-failure';out.mkdir()
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
state=read(campaign/'campaign.json');watch=read(root/'resource-gate.json');rev=state['source'];assert rev=='2ef3e8bea19c233f14c12564b4941b721913ba6b'
assert state['status']=='failed' and state['exit_code']==1 and state['current_seed']==2 and state['seeds']==list(range(1,201)) and [r['seed'] for r in state['records']]==[1,2] and [r['exit_code'] for r in state['records']]==[0,1]
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show','js-wf-corrected-partition200-v4-20261008.service','--property=MainPID,ExecMainPID,Result,ExecMainStatus,InvocationID,ActiveState,SubState,RuntimeMaxUSec,MemoryMax,Restart,ExecMainStartTimestamp,ExecMainExitTimestamp'],text=True).splitlines())
assert unit['MainPID']=='0' and unit['Result']=='exit-code' and unit['ExecMainStatus']=='1' and unit['InvocationID']=='72f514b866d643b59a3b07b7cca09bc1' and unit['ExecMainPID']==str(watch['supervisor_pid']) and unit['SubState']=='failed' and unit['Restart']=='no'
assert state['producer_pid']==watch['campaign_producer_pid'] and watch['exit_code']==1
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source') and subprocess.check_output(['git','rev-parse','HEAD'],cwd=root/'source',text=True).strip()==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();go=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum')]
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in go).encode(),cwd=repo));sourcehashes={}
for n in go:
 h=stream.readline().split();assert h[1]==b'blob';data=stream.read(int(h[2]));assert stream.read(1)==b'\n';sourcehashes[n]=hashlib.sha256(data).hexdigest();assert sha(root/'source'/n)==sourcehashes[n]
assert not stream.read()
closed_pids=[int(unit['ExecMainPID']),state['producer_pid']];seeds=[]
for record in state['records']:
 d=campaign/record['root'];e=read(d/'execution.json');a=read(d/'source-before.json');assert a==read(d/'source-after.json') and a['revision']==rev and a['files']==sourcehashes
 for name,h in sourcehashes.items():assert sha(d/'source'/name)==h
 external=read(d/'external-source-before.json');assert external==read(d/'external-source-after.json');paths=read(d/'external-captured-paths.json');assert set(paths)==set(external)
 for name,h in external.items():assert sha(d/paths[name])==h
 for name,h in record['record_sha256'].items():assert sha(d/name)==h
 cmd=read(d/'commands.json');assert cmd['test_command']==e['actual_argv']==[str(d/'integration.test'),'-test.run=^TestMixedMatrixServerPartitionEveryThirtySeconds$','-test.count=1','-test.v','-test.timeout=18m']
 assert e['source']==rev and e['exit_code']==record['exit_code'] and e['duration']=='10m' and e['race'] is False and e['server_observer_errors']==0 and e['environment']['GOMAXPROCS']=='2' and e['environment']['GOMEMLIMIT']=='2GiB' and e['environment']['FAULT_SEED']==str(record['seed'])
 assert sha(d/'integration.test')==e['sha256'] and 'vcs.revision='+rev in e['build_info'] and 'vcs.modified=false' in e['build_info'] and '-race=true' not in e['build_info']
 closed_pids.append(e['pid']);peers=read(d/'observed-servers.json');assert len(peers)==3 and {p['node'] for p in peers}=={0,1,2}
 expected=review.candidate_binding(d,rev,Path('docs/scale/lease-partition-component-2026-10-06/contiguous-component'))
 for peer in peers:
  assert peer['actual_executable_sha256']==expected and sha(d/peer['captured'])==expected and peer['argv'][0]==str(d/'partition-server.bin');closed_pids.append(peer['pid'])
 seeds.append(dict(seed=record['seed'],execution=e,go_inputs=len(go),external_inputs=len(external),candidate_sha256=expected,actual_peers=peers))
assert all(not Path('/proc',str(pid)).exists() for pid in closed_pids)
log=(seed/'native.log').read_text();assert '--- FAIL: TestMixedMatrixServerPartitionEveryThirtySeconds (634.30s)' in log and 'matrixfanout terminal p99=30.188460653s, want <30s' in log and 'MATRIX_TERMINAL_COHORT cutoff=1988 expected_invocations=1988 visited_invocations=1988' in log and 'MATRIX_RETAINED row=server_partition report={Invocations:1988 Journals:1988 Entries:21941 Terminal:1988} expected_invocations=1988' in log
samples=read(seed/'matrix-partition-2-latencies.json');faults=read(seed/'matrix-partition-2-faults.json')['faults'];assert len(faults)==19
for f in faults:raw.check_fault_identity(f,'partition');assert raw.timestamp_ns(f['scheduled'])<=raw.timestamp_ns(f['killed'])<raw.timestamp_ns(f['healed'])
terminal=[x for x in samples if x['event']=='terminal'];assert len(terminal)==1988
for s in samples:assert type(s['delay_ns']) is int and s['delay_ns']==raw.timestamp_ns(s['observed'])-raw.timestamp_ns(s['enabled']) and s['delay_ns']>=0
fanout=sorted([s for s in terminal if s['type']=='matrixfanout'],key=lambda s:s['delay_ns']);assert len(fanout)==71
p99=fanout[math.ceil(len(fanout)*.99)-1];assert p99['delay_ns']==30188460653
intersect=[f for f in faults if raw.timestamp_ns(f['killed'])<raw.timestamp_ns(p99['observed']) and raw.timestamp_ns(f['healed'])>raw.timestamp_ns(p99['enabled'])];assert len(intersect)==1
post_heal=raw.timestamp_ns(p99['observed'])-max(raw.timestamp_ns(p99['enabled']),raw.timestamp_ns(intersect[0]['healed']))
# Diagnostic only: this majority-preserving row retains the raw-delay gate.
dispatch=[json.loads(x) for x in (seed/'matrix-partition-2-dispatch.jsonl').read_text().splitlines()];operations=[json.loads(x) for x in (seed/'matrix-partition-2-operations.jsonl').read_text().splitlines()]
selected_dispatch=[x for x in dispatch if x['Type']==p99['type'] and x['ID']==p99['id']];selected_operations=[x for x in operations if x['Type']==p99['type'] and x['ID']==p99['id']]
for n,v in [('outlier-dispatch.json',selected_dispatch),('outlier-operations.json',selected_operations),('campaign-terminal.json',state),('resource-gate-terminal.json',watch)]: (out/n).write_text(json.dumps(v,indent=2)+'\n')
for name in ['execution.json','commands.json','source-before.json','source-after.json','native.log','acceptance.json','acceptance.log','partition-server-verification.json']:shutil.copyfile(seed/name,out/name)
initial=closure(root);meta=fixture_archive.capture(root,Path('/tmp/js-wf-corrected-partition200-failed-complete-20261008.tar.gz'),out,compresslevel=1);final=closure(root)
shutil.copyfile(__file__,out/'executed-review.py')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=rev,unit=unit,original_processes_closed=closed_pids,seeds=seeds,native_failure_preserved=True,raw_latency_samples_verified=len(samples),terminal_population_verified=len(terminal),fanout_population=71,offending_sample=p99,overlapping_majority_fault=intersect[0],diagnostic_post_heal_ns=post_heal,raw_gate_applies=True,cause_unconfirmed=True,complete_archive=meta,closure_before=initial,closure_after=final,scope='Original stopped corrected campaign: seed1 accepted separately, seed2 failed and seeds3–200 unexecuted. Raw latency/timestamps/full selected source/actual binary identities and all local media preserved; retained counts are native assertions, not independent disk-store reconstruction. Quorum-loss heal-time exception does not apply to this majority-progress row. No acceptance, target change or retry.')
(out/'independent-failure-review.json').write_text(json.dumps(report,indent=2)+'\n')
print('CORRECTED_CAMPAIGN_FAILURE_PRESERVED',len(samples),post_heal,meta['archive_bytes'],flush=True)
