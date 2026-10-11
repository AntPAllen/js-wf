"""Review preparation diagnostic fidelity, unchanged model corpus and six SDK exits."""
import hashlib,io,json,re,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent;repo=out.parents[2]
sha=lambda b:hashlib.sha256(b).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
for n,h in json.loads((out/'qualification-inputs.json').read_text()).items():assert sha(Path(n).read_bytes())==h,n
for n,h in inputs.items():assert sha((repo/n).read_bytes())==h,n
unchanged=[n for n in inputs if n.startswith('sim/') and (n.endswith('.go') or n.startswith('sim/testdata/regressions/'))]
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join('d322671:'+n+'\n' for n in unchanged).encode()))
for n in unchanged:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert sha(data)==inputs[n],n
pins=list((repo/'sim/testdata/regressions').glob('*.json'));assert len(pins)==946
rows=[]
for mode in ('normal','race'):
 for label in ('protocol','client','pins'):
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
  else:
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
result=dict(accepted=True,source_inputs=len(inputs),actual_sdk_executions=len(rows),rows=rows,unchanged_simulation_inputs=len(unchanged),pins_unchanged=946,new_preparation_controls_per_mode=23,ci_python_blocks_compiled=len(blocks),native_timeout_cause_established=False,full_scale_accepted=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
