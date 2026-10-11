import hashlib,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
original=subprocess.check_output(['git','show','7fffe3a:worker/graph_continuation_sdk_test.go'])
assert Path('worker/graph_continuation_sdk_test.go').read_bytes()==original.replace(b'if childFailure || pendingChild {\n\t\tobserve =',b'if childPromiseFlow || pendingChild {\n\t\tobserve =')
assert Path('internal/graphpublication/append.go').read_bytes()==subprocess.check_output(['git','show','7fffe3a:internal/graphpublication/append.go'])
modes=('grant_read','cas_not_committed','cas_committed_readback','cas_committed_unknown','cas_committed_cancelled','cas_committed_replaced')
counts={}
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text())
 assert receipt['actual_exit']==0 and receipt['end_ns']>receipt['start_ns']
 assert ('-race' in receipt['command'])==(mode=='race')
 log=(out/(mode+'.log')).read_text()
 assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
 for case in modes:assert f'--- PASS: TestGraphAppendRequestDeadlineRecovery/{case} ' in log
 for test in ('TestGraphPublicationUnknownRepliesNeverAdoptUploads','TestGraphPublicationCollectorAndCommitInterleavings','TestGraphPublicationUncertainCensusFailsClosed','TestGraphPublicationContextAndCopies'):
  assert f'--- PASS: {test} ' in log
 counts[mode]=len(re.findall(r'^\s*--- PASS:',log,re.M))
for mutant,case in (('skip_grant','grant_read'),('skip_witness','cas_committed_replaced')):
 receipt=json.loads((out/(mutant+'-command.json')).read_text());assert receipt['actual_exit']==1
 log=(out/(mutant+'.log')).read_text()
 assert f'--- FAIL: TestGraphAppendRequestDeadlineRecovery/{case} ' in log
 assert log.count('uncertain request incorrectly accepted')==1
 assert len(re.findall(r'^\s*--- PASS: TestGraphAppendRequestDeadlineRecovery/',log,re.M))==5
 overlay=json.loads((out/(mutant+'-overlay.json')).read_text())
 assert overlay=={'Replace':{str(Path('internal/graphpublication/append.go').resolve()):str(out/(mutant+'-append.go.txt'))}}
assert json.loads((out/'observer-build-command.json').read_text())['actual_exit']==0
result=dict(accepted_component_controls=True,source_go_inputs=len(inputs),normal_race_deadline_controls=6,passed_named_tests=counts,mutants_killed=2,production_append_protocol_unchanged=True,native_observer_compile_pass=True,native_rerun=False,native_failure_cause_confirmed=False,full_current_qualification=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
