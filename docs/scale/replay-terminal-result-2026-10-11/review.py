import hashlib,json,re,subprocess,textwrap
from pathlib import Path
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
for name,sha in inputs.items():
    if name=='.github/workflows/graph-publication.yml':
        assert hashlib.sha256((out/'workflow-at-execution.yml.txt').read_bytes()).hexdigest()==sha
    else:
        assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
cases=('inline','user_null','opaque_user_duplicates','empty_success','external','failed','completed_error','completed_limit_request','completed_limit_entry','failed_empty','failed_inline','failed_external','inline_external','hash_without_ref','ref_without_hash','hash_uppercase','hash_short','object_missing','object_wrong')
counts={}; executions={}
for mode in ('normal-admission','race-admission','race-sdk','race-native-plugin'):
    receipt=json.loads((out/(mode+'-command.json')).read_text())
    assert receipt['actual_exit']==0 and receipt['end_ns']>receipt['start_ns']
    assert receipt['go_inputs_unchanged']
    assert ('-race' in receipt['command'])==mode.startswith('race')
    log=(out/(mode+'.log')).read_text()
    assert not re.search(r'^\s*--- FAIL:',log,re.M),mode
    counts[mode]=len(re.findall(r'^\s*--- PASS:',log,re.M))
    own=[e for e in receipt['executions'].values() if receipt['start_ns']<=e['start_ns']<receipt['end_ns']]
    assert len(own)==(4 if mode.endswith('admission') else 1)
    for e in own:
        assert e['actual_exit']==0 and e['end_ns']>e['start_ns']
        binary=Path(e['retained_binary']);data=binary.read_bytes()
        assert len(data)==e['bytes'] and hashlib.sha256(data).hexdigest()==e['sha256']
        build=subprocess.check_output(['go','version','-m',str(binary)],text=True)
        assert ('-race=true' in build)==mode.startswith('race')
        executions[e['pid']]=e
    if mode.endswith('admission') or mode=='race-sdk':
        for identity in ('legacy','identified','graph'):
            for case in cases:
                assert f'--- PASS: TestReplayTerminalResultBeforeHandler/{identity}/{case} ' in log
    if mode.endswith('admission') or mode=='race-native-plugin':
        for format in ('','graph-v1'):
            for case in ('completed_error','failed_inline','failed_external','object_missing','object_wrong'):
                assert f'--- PASS: TestReplayTerminalResultBeforePlugin/format={format}/{case} ' in log
    if mode=='race-native-plugin':
        assert '--- PASS: TestReplayContinuationPlugin ' in log
        assert re.search(r'two continuation plugin stages: raw_entries=\d+ objects=\d+ result=60 offline_effects=0',log)
baseline=(out/'baseline.log').read_text()
assert json.loads((out/'baseline-command.json').read_text())['actual_exit']==1
assert len(re.findall(r'^        --- FAIL:',baseline,re.M))==37
assert baseline.count('handler calls=1')==37 and baseline.count('export admission=<nil>')==37
workflow=Path('.github/workflows/graph-publication.yml').read_text()
assert hashlib.sha256(workflow.encode()).hexdigest()==json.loads((out/'workflow-final-sha256.json').read_text())['sha256']
assert json.loads((out/'ci-before-fix-check.json').read_text())['actual_exit']==1
assert 'IndentationError' in (out/'ci-before-fix-check.log').read_text()
for name,sha in json.loads((out/'baseline-production-inputs.json').read_text()).items():
    original=subprocess.check_output(['git','show','dbcf74976ff000fbe14f6794ef75cab8d30a3a16:'+name])
    assert hashlib.sha256(original).hexdigest()==sha
blocks=re.findall(r"^[ ]*python3 - <<'PY'\n(.*?)^[ ]*PY$",workflow,re.M|re.S)
for block in blocks:compile(textwrap.dedent(block),'<workflow-python>','exec')
terminal=[textwrap.dedent(b) for b in blocks if 'TestReplayTerminalResultBeforeHandler/{identity}/{case}' in b]
assert len(terminal)==1
for mode in ('normal-admission','race-admission'):
    code=terminal[0].replace("pathlib.Path('terminal-outcome-admission.log')",'pathlib.Path('+repr(str(out/(mode+'.log')))+')')
    assert subprocess.run(['python3','-c',code],check=True).returncode==0
result=dict(accepted_replay_admission=True,source_inputs=len(inputs),captured_executions=len(executions),named_passes=counts,sdk_controls=57,cli_controls=10,baseline_bad_histories_entering_handler=37,workflow_python_blocks_compiled=len(blocks),workflow_python_admission_reducers_verified=True,workflow_only_corrected_after_execution=True,full_current_simulation_qualified=False,canonical_native_matrix_qualified=False,native_deadline_cause_confirmed=False,public_continuations_enabled=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
