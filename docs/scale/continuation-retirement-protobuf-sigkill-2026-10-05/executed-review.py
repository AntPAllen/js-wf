from pathlib import Path
import hashlib,json,subprocess,re,shutil
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-retirement-protobuf-to-json-sigkill-race-20261005')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((r/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0
b=json.loads((r/'binary.json').read_text());assert b['sha256']==e['exe_sha256']==sha(r/'integrity.test');info=subprocess.check_output(['go','version','-m',str(r/'integrity.test')],text=True);assert info==b['build_info'] and '-race=true' in info and f"vcs.revision={e['revision']}" in info and 'vcs.modified=false' in info
before=json.loads((r/'source-before.json').read_text());after=json.loads((r/'source-after.json').read_text());assert before==after and before['revision']==e['revision']
for n,d in before['files'].items():assert sha(r/'source'/n)==sha(repo/n)==hashlib.sha256(subprocess.check_output(['git','show',e['revision']+':'+n],cwd=repo)).hexdigest()==d
servers=json.loads((r/'originals/actual-servers.json').read_text());assert len(servers)==3 and {x['node'] for x in servers}=={0,1,2} and len({x['pid'] for x in servers})==3 and len({x['sha256'] for x in servers})==1
server=r/'originals/cluster/nats-server';assert sha(server)==servers[0]['sha256'];server_info=subprocess.check_output(['go','version','-m',str(server)],text=True).splitlines()[1:]
for s in servers:assert s['build_info'].splitlines()[1:]==server_info and s['args'][0]==str(server)
k=json.loads((r/'originals/kill-admission.json').read_text());q=json.loads((r/'originals/result.json').read_text())
assert k['signal']=='SIGKILL' and k['cut']=='after_manifest' and k['child_live_sdk_sha256']==k['parent_sdk_sha256']==e['exe_sha256'] and k['child_pid']!=e['pid'] and k['lease_ttl']=='12s'
assert q['old_generation']==k['old_generation']==1 and q['fresh_generation']==k['fresh_generation']==3 and q['held_epoch']==k['held_epoch'] and q['terminal_epoch']>q['held_epoch']>0
assert q['reclaimed_objects']==k['reclaimed_objects']==2 and 0<q['recovery_seconds']<30 and q['archive_reads']==0 and q['frame_reads']>0 and q['effects']==3 and q['terminals']==q['invocations']==2 and q['shared_and_survivor_and_fresh_references_verified'] and q['all_peer_fresh_results_verified']
ledger=(r/'originals/effects.log').read_text().splitlines();counts={line:ledger.count(line) for line in set(ledger)};assert counts==q['ledger_counts']=={'initial:1':2,'effect:1':2,'stage:1':2,'initial:2':1,'effect:2':1,'stage:2':1}
assert q['child_encoding']=='protobuf-v1' and q['successor_encoding']=='json' and q['bounded_resume_required'] and q['read_counters_observed']
schema='protocol/v1/journal.proto';schema_bytes=subprocess.check_output(['git','show',e['revision']+':'+schema],cwd=repo);assert schema_bytes==(repo/schema).read_bytes()
p=r/'python-codec-source'/schema;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(schema_bytes)
generated=r/'generated';generated.mkdir(exist_ok=True)
command=['protoc','--python_out='+str(generated),schema];subprocess.run(command,cwd=r/'python-codec-source',check=True)
import sys,google.protobuf
sys.path.insert(0,str(generated))
from protocol.v1 import journal_pb2
raw=(r/'originals/protobuf-anchor.bin').read_bytes();assert raw.startswith(b'WFJ\x00')
anchor=journal_pb2.JournalRecord();anchor.ParseFromString(raw[4:]);admission=json.loads((r/'originals/encoding-admission.json').read_text())
assert anchor.version==1 and anchor.sequence==0 and anchor.entry.kind==journal_pb2.ENTRY_KIND_STEP_COMPLETED and anchor.entry.epoch==k['held_epoch']==admission['epoch'] and anchor.entry.worker_id=='retirement-killed' and sha(r/'originals/protobuf-anchor.bin')==admission['sha256']
payload=json.loads(anchor.entry.payload_json);assert payload['result_ref'] and payload['result_hash']
terminal=json.loads((r/'originals/json-terminal.json').read_text());assert terminal['kind']=='Completed' and terminal['epoch']==q['terminal_epoch'] and terminal['worker_id']=='retirement-successor'
codec_review={'schema_sha256':hashlib.sha256(schema_bytes).hexdigest(),'schema_matches_recorded_git':True,'protoc_version':subprocess.check_output(['protoc','--version'],text=True).strip(),'python_protobuf_version':google.protobuf.__version__,'generated_codec_sha256':sha(generated/'protocol/v1/journal_pb2.py'),'actual_protobuf_anchor_decoded':True,'anchor_epoch':anchor.entry.epoch,'anchor_index':anchor.entry.index,'anchor_payload':payload,'actual_json_terminal_decoded':True,'terminal':terminal}
(r/'encoding-review.json').write_text(json.dumps(codec_review,indent=2)+'\n')
log=(r/'native.log').read_text();assert '\nPASS\n' in log;elapsed=float(re.search(r'--- PASS: TestContinuationRetirementProtobufWorkerSIGKILLToJSONSuccessor \(([0-9.]+)s\)',log)[1])
review={'source':e['revision'],'actual_live_parent_sdk_sha256':e['exe_sha256'],'actual_live_child_sdk_sha256':k['child_live_sdk_sha256'],'clean_full_build_info_and_race_verified':True,'source_inputs':len(before['files']),'source_before_after_and_git_match':True,'actual_native_server_processes':3,'actual_native_server_sha256':servers[0]['sha256'],'all_live_server_and_retained_binary_build_fields_match':True,'mixed_encoding_independent_python_decode':codec_review,'named_pass_seconds':elapsed,'kill_admission':k,'result':q,'exact_effect_ledger_verified':True,'kill_and_fence_scope':'Named-test actual reaped SIGKILL WaitStatus, published fresh generation checkpoint and held owner before cut, terminal epoch above held epoch after successor recovery','physical_stores_scope':'Stopped originals archived/hash-verified, not independently reopened','scope':'Focused retirement/quiescent-GC/generation-reuse plus fresh-manifest protobuf worker SIGKILL to JSON successor; not every retirement timing cut, server simultaneous outage, online GC, domain/legacy faults, full matrices or24h qualification. Production TTL12/heartbeat3/AckWait13 and original30s startup/60s scenario/under30 recovery unchanged; Tier1 graph unchanged'}
(r/'qualification.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,r/'executed-review.py');print('QUALIFIED',elapsed,q['recovery_seconds'],len(before['files']),q['held_epoch'],q['terminal_epoch'])
