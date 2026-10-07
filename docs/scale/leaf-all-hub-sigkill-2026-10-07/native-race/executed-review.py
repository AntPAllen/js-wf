import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,subprocess,importlib.util,tarfile,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-leaf-all-sigkill-20261007');proof=Path('/tmp/js-wf-leaf-all-sigkill-independent-20261007')
def read(p):return json.loads(p.read_text())
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
execution=read(root/'execution.json');before=read(root/'source-before.json');after=read(root/'source-after.json');assert before==after and execution['source']==before['revision']
for name,digest in before['files'].items():
 assert sha(root/'selected-source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',execution['source']+':'+name],cwd=repo)).hexdigest()==digest
actual=read(root/'actual-sdk.json');binary=read(root/'binary.json');command=read(root/'commands.json')['run']
assert actual['exe_sha256']==binary['sha256']==sha(root/'integration-race.test') and actual['args']==command
assert actual['admission']['stable_identity_observed_twice'] and not Path('/proc',str(actual['pid'])).exists()
assert '-race=true' in binary['build_info'] and 'vcs.revision='+execution['source'] in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
assert not any(token in binary['build_info'] for token in ('-overlay=', '-modfile='))
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0\t' in binary['build_info']
assert actual['environment']['GOMAXPROCS']=='2' and actual['environment']['GOMEMLIMIT']=='1GiB'
assert actual['environment']['WF_CONTINUATION_DOMAIN_ROOT']==str(root/'stores/leaf-scenario')
proof.mkdir();sys.path.insert(0,str(repo/'scripts'));import fixture_archive,leaf_domain_proof
spec=importlib.util.spec_from_file_location('row',repo/'scripts/run-domain-runtime-controls.py');row=importlib.util.module_from_spec(spec);spec.loader.exec_module(row)
archive_meta=read(Path('/tmp/js-wf-leaf-all-sigkill-20261007-proof')/'archive-verification.json');inventory=read(Path('/tmp/js-wf-leaf-all-sigkill-20261007-proof')/'fixture-inventory.json')
with Path('/tmp/js-wf-leaf-all-sigkill-20261007.tar.gz').open('rb') as f:
 declared,compressed=fixture_archive.verify_hashed_stream(f,dict(bytes=archive_meta['archive_bytes'],sha256=archive_meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
report={'build_vcs_stamp_present': 'vcs.revision=' in binary['build_info'], 'source_binding':'Verified complete selected source before/after, captured files and Git blobs plus recorded compiler command and actual live SDK digest; actual race binary buildVCS matches the executed source.', 'source':execution['source'],'source_inputs_verified':len(before['files']),'actual_sdk':actual,'broker_scope':'Three stock hub processes and one stock leaf process, eight total incarnations, verified against their original/replacement executables and argv. Race instrumentation covers the SDK and fixture code, not the separate stock brokers. Module version is provenance, not a complete external-source census.','archive_every_member_verified':compressed,'files':len(inventory['files']),'accepted':False,'original_stores_not_reopened':True,'full_matrix_24h_qualified':False}
log=(root/'native.log').read_text()
if execution['exit_code']==0:
 report['log_review']=row.verify_log('leaf-retirement-all-sigkill-expiry-weak',log)
 report['leaf_review']=leaf_domain_proof.validate(read(root/'stores/leaf-scenario/leaf-domain-proof.json'),'leaf-and-all-hub-sigkill-lease-expiry-weak-frame',log)
 assert read(root/'row-review.json')['rejection'] is None
 report['accepted']=True
 servers=read(root/'actual-servers.json');assert len(servers)==8
 leaf_record=read(root/'stores/leaf-scenario/leaf-domain-proof.json')
 observed={native['pid']:native for native in servers};assert len(observed)==8
 expected=leaf_record['hub_pids_before']+leaf_record['hub_pids_after']+[leaf_record['leaf_pid_before'],leaf_record['leaf_pid_after']]
 assert set(observed)==set(expected)
 pairs=list(zip(leaf_record['hub_pids_before'],leaf_record['hub_pids_after']))+[(leaf_record['leaf_pid_before'],leaf_record['leaf_pid_after'])]
 for old,new in pairs:
  assert observed[old]['args']==observed[new]['args'] and observed[old]['exe_sha256']==observed[new]['exe_sha256']
 for i,(old,new) in enumerate(pairs):
  retained=root/'stores/leaf-scenario'/('hub-processes' if i<3 else 'leaf-process')/'nats-server'
  for pid in (old,new):
   native=observed[pid]
   assert native['exe']==str(retained) and native['exe_sha256']==sha(retained) and not Path('/proc',str(pid)).exists()
   assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in native['build_info']
  args=observed[old]['args']
  assert args[args.index('-n')+1]==f'wf-process-{i if i<3 else 0}'
 report['actual_stock_hub_and_leaf_incarnations']=servers
 report['same_original_replacement_executable_argv_config_ports_and_store']=True
 admission=read(root/'stores/leaf-scenario/hub-metadata-ready.json')
 assert admission['peers']==['wf-process-0','wf-process-1','wf-process-2'] and admission['leader'] in admission['peers'] and admission['all_hubs_current'] is True
 states=admission['process_metadata'];assert len(states)==3
 assert {state['server_id'] for state in states}=={peer['id'] for peer in leaf_record['before']}
 for state in states:
  assert not state.get('disabled',False) and state['config']['domain']=='WFRETIRE'
  assert state['meta_cluster']['leader']==admission['leader'] and state['meta_cluster']['cluster_size']==3 and not state['meta_cluster'].get('rescue',False)
 leader_node=next(i for i,peer in enumerate(leaf_record['before']) if peer['name']==admission['leader'])
 replicas=states[leader_node]['meta_cluster']['replicas'];assert len(replicas)==2
 assert {replica['name'] for replica in replicas}==set(admission['peers'])-{admission['leader']}
 assert all(replica['current'] and not replica.get('offline',False) and replica.get('lag',0)==0 for replica in replicas)
 report['hub_metadata_admission']=admission
 # Controls mutate the actual proof, keeping the native positive fixture fixed.
 import copy
 baseline=read(root/'stores/leaf-scenario/leaf-domain-proof.json');bad=[]
 def variant(fn):
  item=copy.deepcopy(baseline);fn(item);bad.append(item)
 variant(lambda p:p.update(scenario_passed=False))
 variant(lambda p:p.update(remote_domain='WFEDGE'))
 variant(lambda p:p.update(local_domain='WFRETIRE'))
 variant(lambda p:p.update(local_streams_after=1))
 variant(lambda p:p.update(leaf_disconnected=False))
 variant(lambda p:p.update(leaf_reconnected=False))
 variant(lambda p:p.update(runtime_server_ids=[p['before'][0]['id']]*3))
 variant(lambda p:p.update(after=p['before']))
 variant(lambda p:p.update(stopped=p['stopped'][:2]))
 variant(lambda p:p['leaf_after'].update(leafnodes=0))
 variant(lambda p:p['leaf_after']['leafs'][0].update(name='wrong'))
 variant(lambda p:p['leaf_after']['leafs'][0].update(account='OTHER'))
 variant(lambda p:p.update(cut_end=p['cut_start']))
 variant(lambda p:p.update(subjects={'$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB':1}))
 variant(lambda p:p.update(subjects={}))
 variant(lambda p:p.update(subjects={k:v for k,v in p['subjects'].items() if '.DIRECT.GET.OBJ_WF_BLOB.' not in k}))
 variant(lambda p:p.update(subjects={k:v for k,v in p['subjects'].items() if '.CONSUMER.CREATE.OBJ_WF_BLOB.' not in k}))
 variant(lambda p:p.update(subjects={k:v for k,v in p['subjects'].items() if '.STREAM.MSG.GET.KV_WF_STATE' not in k}))
 variant(lambda p:p.update(fault_profile='hub-restart'))
 variant(lambda p:p.update(leaf_signal='terminated'))
 variant(lambda p:p.update(leaf_exit_observed=False))
 variant(lambda p:p.update(leaf_pid_after=p['leaf_pid_before']))
 variant(lambda p:p.update(leaf_original_id=p['leaf_id']))
 variant(lambda p:p.update(client_disconnects=[True,True]))
 variant(lambda p:p['leaf_before'].update(server_id=p['leaf_id']))
 variant(lambda p:p.update(lease_ttl_seconds=30))
 variant(lambda p:p.update(prior_epoch=0))
 variant(lambda p:p.update(terminal_epoch=p['prior_epoch']))
 variant(lambda p:p.update(lease_revision=0))
 variant(lambda p:p.update(outage_end=p['outage_start']))
 variant(lambda p:p.update(outage_start=p['cut_end']))
 variant(lambda p:p.update(fault_profile='leaf-sigkill-hub-restart'))
 variant(lambda p:p.update(journal_records=0))
 variant(lambda p:p.update(weak_drops=0))
 variant(lambda p:p.update(weak_reads=1))
 variant(lambda p:p.update(weak_leaders=0))
 variant(lambda p:p.update(weak_direct=1))
 variant(lambda p:p.update(weak_generation=1))
 variant(lambda p:p.update(fresh_generation=p['fresh_generation']+1))
 variant(lambda p:p.update(weak_object='wrong'))
 variant(lambda p:p.update(weak_route='$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB'))
 variant(lambda p:p.update(weak_object='step-result-'+'0'*64))
 variant(lambda p:p.update(hub_pids_before=p['hub_pids_before'][:2]))
 variant(lambda p:p.update(hub_pids_after=p['hub_pids_before']))
 variant(lambda p:p.update(hub_pids_before=[p['leaf_pid_before'],*p['hub_pids_before'][1:]]))
 variant(lambda p:p.update(hub_pids_after=[0,*p['hub_pids_after'][1:]]))
 variant(lambda p:p.update(hub_signals=['terminated']*3))
 variant(lambda p:p.update(hub_exit_observed=[True,False,True]))
 variant(lambda p:p.update(hub_pids_before=list(reversed(p['hub_pids_before']))))
 variant(lambda p:p.update(fault_profile='leaf-sigkill-lease-expiry-weak-frame-hub-restart'))
 for item in bad:
  try:leaf_domain_proof.validate(item,'leaf-and-all-hub-sigkill-lease-expiry-weak-frame',log)
  except (AssertionError,ValueError,KeyError):pass
  else:raise AssertionError('negative actual-proof mutation accepted')
 report['rejected_actual_proof_mutations']=len(bad)
else:report['failure_preserved']=True
(report_path:=proof/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,proof/'executed-review.py')
print('INDEPENDENT_LEAF_REVIEW',report['accepted'],report.get('leaf_review'),flush=True)
