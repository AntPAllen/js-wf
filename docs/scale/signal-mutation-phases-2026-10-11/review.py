"""Check captured source, executable exits and every required component/pin result."""
import hashlib,json,re
from pathlib import Path
out=Path(__file__).resolve().parent
repo=out.parents[2]
sha=lambda b:hashlib.sha256(b).hexdigest()
inputs=json.loads((out/'source-inputs.json').read_text())
for n,h in inputs.items(): assert sha((repo/n).read_bytes())==h,n
pins=list((repo/'sim/testdata/regressions').glob('*.json'))
assert len(pins)==946
rows=[]
for mode in ('normal','race'):
    for package in ('client','sim'):
        label=f'{package}-{mode}'
        receipt=json.loads((out/(label+'-command.json')).read_text())
        assert receipt['actual_exit']==0 and receipt['source_unchanged']
        assert len(receipt['executions'])==1
        execution=next(iter(receipt['executions'].values()))
        assert execution['actual_exit']==0
        assert receipt['start_ns']<=execution['start_ns']<execution['end_ns']<=receipt['end_ns']
        assert not Path('/proc/'+str(execution['pid'])).exists()
        binary=Path(execution['retained_binary']).read_bytes()
        assert len(binary)==execution['bytes'] and sha(binary)==execution['sha256']
        log=(out/(label+'.log')).read_text()
        assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
        assert re.search(r'^PASS$',log,re.M) and re.search(r'^ok\s+js-wf/'+package+r'\s',log,re.M)
        if package=='client':
            for cut in ('reserve_prepare','reserve_commit','reserve_commit_lost','reserve_commit_unknown','binding_prepare','binding_commit','binding_commit_lost','binding_commit_unknown'):
                assert f'--- PASS: TestCanonicalSignalMutationErrorPhases/{cut} ' in log
            for name in ('TestGraphCanonicalSignalBindingBudgetRecovery','TestGraphCanonicalSignalBindingActualDeadlineRecovery','TestCanonicalSignalConflictIsNotGenerationReplacement'):
                assert f'--- PASS: {name} ' in log
        else:
            for pin in pins: assert f'--- PASS: TestPinnedRegressionCorpus/{pin.name} ' in log,pin.name
        rows.append(dict(label=label,actual_exit=0,named_pass=len(re.findall(r'^\s*--- PASS:',log,re.M)),binary_sha256=execution['sha256']))
# Compile each Python heredoc in the CI file, including the new phase reducer.
workflow=(repo/'.github/workflows/graph-publication.yml').read_text().splitlines()
blocks=[]; block=None; delimiter=None
for line in workflow:
    m=re.search(r"python3 - <<'([^']+)'",line)
    if m: block=[]; delimiter=m.group(1); continue
    if block is not None:
        if line.strip()==delimiter:
            compile('\n'.join(block),'CI heredoc','exec'); blocks.append(block); block=None
        else: block.append(line[10:] if line.startswith('          ') else line)
assert block is None
assert any('signal-mutation-phases.log' in '\n'.join(b) for b in blocks)
result=dict(accepted=True,source_inputs=len(inputs),pins_per_mode=len(pins),signal_cuts_per_mode=8,ci_python_blocks_compiled=len(blocks),executions=rows,native_cause_established=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
