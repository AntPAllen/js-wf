#!/usr/bin/env python3
"""Review complete CLI race evidence; skipped opt-in backends remain unqualified."""
import gzip, hashlib, io, json, re, subprocess
from pathlib import Path
here=Path(__file__).resolve().parent
repo=here.parents[2]
state=json.loads((here/'restored-full-state.json').read_text())
assert state['exit']==0 and state['ended_utc'] and state['inputs_unchanged']
assert state['command']==['/usr/local/bin/go','test','-race','./cmd/wf','-timeout=10m','-count=1','-v']
before=json.loads(gzip.decompress((here/'restored-full-inputs-before.json.gz').read_bytes()))
assert before==json.loads(gzip.decompress((here/'restored-full-inputs-after.json.gz').read_bytes()))
names=subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines()
expected={n for n in names if n.endswith('.go') or n in ('go.mod','go.sum') or '/testdata/' in n}
assert set(before)==expected
for name, digest in before.items():
    assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest,name
for name in ('visibility/graph.go','visibility/graph_test.go'):
    assert gzip.decompress((here/('restored-'+name.replace('/','_')+'.txt.gz')).read_bytes())==(repo/name).read_bytes()
assert gzip.decompress((here/'restored-graph_test.go.txt.gz').read_bytes())==(repo/'cmd/wf/graph_test.go').read_bytes()
data=(here/'restored-full-race.log').read_text()
assert hashlib.sha256(data.encode()).hexdigest()==state['log_sha256']
assert re.search(r'^ok\s+js-wf/cmd/wf\s+',data,re.M)
assert not any(marker in data for marker in ('--- FAIL:', 'WARNING: DATA RACE', 'panic:'))
for case in ('R1','R3Domain'):
    assert re.search(r'--- PASS: TestNativeCanonicalGraphOperatorCommands/'+case+r' ',data)
    assert re.search(r'GRAPH_OPERATOR_CALL_END operation=signal domain='+('WFGRAPHOPS' if case=='R3Domain' else '')+r' wall=.*error=<nil>',data)
    assert 'legacy journal requests zero and domain API verified' in data
assert '--- SKIP: TestNativeCanonicalGraphPostgresOperatorCommands' in data
visibility=(here/'visibility-restored-race.log').read_text()
assert re.search(r'^ok\s+js-wf/visibility\s+',visibility,re.M)
assert '--- FAIL:' not in visibility and 'WARNING: DATA RACE' not in visibility
for case in ('completed-lease-held','failed-lease-held','completed-lease-held-history-unknown'):
    assert re.search(r'--- PASS: TestGraphVisibilityCanonicalRowsAndUncertainty/'+case+r' ',visibility)
prior=json.loads((here/'full-state.json').read_text())
prior_log=(here/'full-race.log').read_text()
assert prior['exit']==1 and prior['ended_utc'] and prior['inputs_unchanged']
assert hashlib.sha256(prior_log.encode()).hexdigest()==prior['log_sha256']
assert 'graph view mixed sources {[] } <nil>' in prior_log
assert '--- FAIL: TestNativeCanonicalGraphOperatorCommands/R3Domain' in prior_log
prior_inputs=json.loads(gzip.decompress((here/'full-inputs-before.json.gz').read_bytes()))
assert prior_inputs==json.loads(gzip.decompress((here/'full-inputs-after.json.gz').read_bytes()))
assert set(prior_inputs)==expected
objects=subprocess.check_output(['git','cat-file','--batch'],cwd=repo,
    input=''.join(prior['source_head']+':'+n+'\n' for n in prior_inputs).encode())
stream=io.BytesIO(objects)
for name,digest in prior_inputs.items():
    header=stream.readline().decode().split()
    assert header[1]=='blob'
    content=stream.read(int(header[2])); assert stream.read(1)==b'\n'
    if name=='cmd/wf/graph_test.go':
        content=gzip.decompress((here/'graph_test.go.txt.gz').read_bytes())
    assert hashlib.sha256(content).hexdigest()==digest,name
result=dict(accepted=True,source_head=state['source_head'],source_inputs=len(before),
            package='js-wf/cmd/wf',race=True,wall_seconds=state['wall_seconds'],
            retained_root=state['retained_root'],postgres_qualified=False,
            prior_intermittent_signal_cause_confirmed=False,terminal_lease_projection_model_reproduced=True,actual_100000_entries_qualified=False,
            skipped_tests=re.findall(r'^--- SKIP: (\S+)',data,re.M),log_sha256=state['log_sha256'])
(here/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
