"""Review preparation diagnostic fidelity, unchanged model corpus and six SDK exits."""
import hashlib,io,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2]
sha=lambda b:hashlib.sha256(b).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
for n,h in inputs.items():assert sha((repo/n).read_bytes())==h,n
pins=list((repo/'sim/testdata/regressions').glob('*.json'));assert len(pins)==946
changes={'sim/'+v['Path']:v for v in json.loads((out/'migration.json').read_text())};assert len(changes)==208
names=[str(p.relative_to(repo)) for p in pins]
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join('bec3049:'+n+'\n' for n in names).encode()))
for n in names:
 header=stream.readline().split();assert header[1]==b'blob';old=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 new=(repo/n).read_bytes();a=json.loads(old);b=json.loads(new)
 assert not b.get('guidance_mask')
 assert a['decisions']==b['decisions'],n
 a.pop('transport',None);b.pop('transport',None);assert a==b,n
 if n in changes:
  assert changes[n]['BeforeSHA256']==sha(old) and changes[n]['AfterSHA256']==sha(new) and not changes[n]['ActorScheduleChanged']
 else:assert old==new,n
runner='sim/graph_publication_start_test.go'
old=subprocess.check_output(['git','show','bec3049:'+runner],cwd=repo).decode();new=(repo/runner).read_text()
marker='\tcase "bind_drop", "bind_lost", "unknown_bind":'
a=old.index(marker);b=new.index(marker);ae=old.index('\n\t}\n\tif err != nil',a);be=new.index('\n\t}\n\tif err != nil',b)
assert old[:a]==new[:b] and old[ae:]==new[be:]
assert old[a:ae].count('m.PauseBefore("cas_root"')==3 and new[b:be].count('m.PauseBefore("cas_root"')==2
unchanged=[n for n in inputs if n.startswith('sim/') and n.endswith('.go') and n not in [runner,'sim/graph_start_atomic_binding_test.go']]
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join('bec3049:'+n+'\n' for n in unchanged).encode()))
for n in unchanged:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert sha(data)==inputs[n],n
rows=[]
for mode in ('normal','race'):
 for label in ('protocol','client','journal','pins'):
  name=label+'-'+mode;receipt=json.loads((out/(name+'-command.json')).read_text())
  assert receipt['actual_exit']==0 and receipt['source_unchanged'] and len(receipt['executions'])==1
  execution=next(iter(receipt['executions'].values()));assert execution['actual_exit']==0
  assert receipt['start_ns']<=execution['start_ns']<execution['end_ns']<=receipt['end_ns']
  assert not Path('/proc/'+str(execution['pid'])).exists() and not Path('/proc/'+str(receipt['pid'])).exists()
  binary=Path(execution['retained_binary']);assert sha(binary.read_bytes())==execution['sha256'] and binary.stat().st_size==execution['bytes']
  assert ('-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True))==(mode=='race')
  log=(out/(name+'.log')).read_text();assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M) and re.search(r'^PASS$',log,re.M)
  if label=='protocol':
   cuts=re.findall(r'\{name: "([^"]+)"', (repo/'internal/graphpublication/prepare_error_test.go').read_text())
   assert len(cuts)==23
   for cut in cuts:assert f'--- PASS: TestGraphAppendPreparationErrorPhases/{cut} ' in log
   for cut in ('success','stale_head','missing_pin','forged_snapshot','oversized','cancelled_before','conflict','rejected','committed_lost','committed_unknown','committed_replaced','committed_cancelled'):assert f'--- PASS: TestGraphApplicationAndReaderReleaseAtomic/{cut} ' in log
   for top in re.findall(r'^func (TestGraphPublication\w+)\(', (repo/'internal/graphpublication/protocol_test.go').read_text(), re.M):assert f'--- PASS: {top} ' in log
   for cut in ('context','plan','observe','conflict','preservation','graph','node_grant','payload_grant','owned_payload','root_rejected','root_lost','root_unknown','root_cancelled','conflict_readback_unknown'):
    assert f'--- PASS: TestGraphAppendCommitErrorPhases/{cut} ' in log
   for cut in ('grant_read','cas_not_committed','cas_committed_readback','cas_committed_unknown','cas_committed_cancelled','cas_committed_replaced'):
    assert f'--- PASS: TestGraphAppendRequestDeadlineRecovery/{cut} ' in log
  elif label=='client':
   for cut in ('reserve_prepare','reserve_commit','reserve_commit_lost','reserve_commit_unknown','binding_prepare','binding_commit','binding_commit_lost','binding_commit_unknown'):
    assert f'--- PASS: TestCanonicalSignalMutationErrorPhases/{cut} ' in log
   for cut in ('binding_commit','binding_commit_unknown'):
    for window in ('within','boundary'):assert f'--- PASS: TestCanonicalSignalRecoveryAfterDedupExpiry/{cut}/{window} ' in log
   for top in ('TestGraphCanonicalSignalBindingBudgetRecovery','TestGraphCanonicalSignalBindingActualDeadlineRecovery','TestCanonicalSignalConflictIsNotGenerationReplacement'):
    assert f'--- PASS: {top} ' in log
  elif label=='journal':
   for top in ('TestGraphCanonicalStartPendingBindingAndRetainedInput','TestGraphCanonicalStartRejectsLegacyAndBounds'):assert f'--- PASS: {top} ' in log
  else:
   for cut in ('success','cleanup_observation','wrong_token','corrupt_input','conflict','rejected','committed_lost','committed_unknown'):assert f'--- PASS: TestGraphStartBindingAndReaderReleaseAtomic/{cut} ' in log
   for pin in pins:assert f'--- PASS: TestPinnedRegressionCorpus/{pin.name} ' in log,pin.name
  rows.append(dict(name=name,actual_exit=0,named_pass=len(re.findall(r'^\s*--- PASS:',log,re.M)),binary_sha256=execution['sha256']))
block=None;blocks=[]
for line in (repo/'.github/workflows/graph-publication.yml').read_text().splitlines():
 m=re.search(r"python3 - <<'([^']+)'",line)
 if m:block=[];delimiter=m.group(1);continue
 if block is not None:
  if line.strip()==delimiter:compile('\n'.join(block),'CI Python block','exec');blocks.append(block);block=None
  else:block.append(line[10:] if line.startswith('          ') else line)
assert block is None and any('append-commit-phases.log' in '\n'.join(b) for b in blocks)
result=dict(accepted=True,source_inputs=len(inputs),actual_sdk_executions=len(rows),rows=rows,unchanged_simulation_inputs=len(unchanged),pins=946,pins_migrated=208,original_decisions_preserved=946,new_atomic_protocol_controls=12,new_binding_controls=8,ci_python_blocks_compiled=len(blocks),native_timeout_cause_established=False,full_scale_accepted=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
