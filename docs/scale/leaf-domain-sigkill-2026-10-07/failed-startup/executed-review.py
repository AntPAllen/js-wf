import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,subprocess,importlib.util,tarfile,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-leaf-domain-sigkill-20261007');proof=Path('/tmp/js-wf-leaf-domain-sigkill-independent-20261007')
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
archive_meta=read(Path('/tmp/js-wf-leaf-domain-sigkill-20261007-proof')/'archive-verification.json');inventory=read(Path('/tmp/js-wf-leaf-domain-sigkill-20261007-proof')/'fixture-inventory.json')
with Path('/tmp/js-wf-leaf-domain-sigkill-20261007.tar.gz').open('rb') as f:
 declared,compressed=fixture_archive.verify_hashed_stream(f,dict(bytes=archive_meta['archive_bytes'],sha256=archive_meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
report={'build_vcs_stamp_present': 'vcs.revision=' in binary['build_info'], 'source_binding':'Verified complete selected source before/after, captured files and Git blobs plus recorded compiler command and actual live SDK digest; actual race binary buildVCS matches the executed source.', 'source':execution['source'],'source_inputs_verified':len(before['files']),'actual_sdk':actual,'embedded_brokers':'Three hub NATS library servers run in the admitted race SDK. Separate leaf process identities and native SIGKILL are verified only for an accepted row; missing short-lived process observations in a startup failure are not inferred. Module version is build-info provenance, not a complete prebuild external-source census.','archive_every_member_verified':compressed,'files':len(inventory['files']),'accepted':False,'original_stores_not_reopened':True,'full_matrix_24h_qualified':False}
log=(root/'native.log').read_text()
if execution['exit_code']==0:
 report['log_review']=row.verify_log('leaf-retirement-sigkill-hub-restart',log)
 report['leaf_review']=leaf_domain_proof.validate(read(root/'stores/leaf-scenario/leaf-domain-proof.json'),'leaf-sigkill-hub-restart')
 assert read(root/'row-review.json')['rejection'] is None
 report['accepted']=True
 servers=read(root/'actual-servers.json');assert len(servers)==2
 leaf_record=read(root/'stores/leaf-scenario/leaf-domain-proof.json')
 assert {x['pid'] for x in servers}=={leaf_record['leaf_pid_before'],leaf_record['leaf_pid_after']}
 retained_sha=sha(root/'stores/leaf-scenario/leaf-process/nats-server')
 for native in servers:
  assert native['exe_sha256']==retained_sha and not Path('/proc',str(native['pid'])).exists()
  assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in native['build_info']
 assert servers[0]['args']==servers[1]['args']
 report['actual_stock_leaf_incarnations']=servers
 report['same_leaf_executable_argv_and_store']=True
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
 for item in bad:
  try:leaf_domain_proof.validate(item,'leaf-sigkill-hub-restart')
  except (AssertionError,ValueError,KeyError):pass
  else:raise AssertionError('negative actual-proof mutation accepted')
 report['rejected_actual_proof_mutations']=len(bad)
else:report['failure_preserved']=True
(report_path:=proof/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
shutil.copyfile(__file__,proof/'executed-review.py')
print('INDEPENDENT_LEAF_REVIEW',report['accepted'],report.get('leaf_review'),flush=True)
