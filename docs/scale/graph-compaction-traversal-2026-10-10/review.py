"""Review actual frozen component completion, including exact source and seeds."""
from collections import Counter
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
state = json.loads((base/'qualification-state.json').read_text())
root, checkout = Path(state['root']), Path(state['checkout'])
assert state['phase'] == 'closed' and state['exit'] == 0 and state['finished']
props = subprocess.check_output(['systemctl', '--user', 'show', 'js-wf-compaction-traversal-qualification-20261010.service',
    '-p', 'LoadState', '-p', 'MainPID', '-p', 'ExecMainStatus', '-p', 'InvocationID'], text=True)
assert 'LoadState=loaded\n' in props and 'MainPID=0\n' in props and 'ExecMainStatus=0\n' in props
assert 'InvocationID='+state['invocation']+'\n' in props
(root/'supervisor-exit.txt').write_text(props)
assert hashlib.sha256((root/'executed-run.py').read_bytes()).hexdigest() == state['driver_sha256']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == state['source']
before, after = [json.loads((root/n).read_text()) for n in ('source-before.json', 'source-after.json')]
assert before == after and before['source'] == state['source']
names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', state['source']], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
assert set(names) == set(before['files'])
stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=checkout,
    input=''.join(state['source']+':'+n+'\n' for n in names).encode()))
for n in names:
    header = stream.readline().split()
    assert header[1] == b'blob'
    body = stream.read(int(header[2]))
    assert stream.read(1) == b'\n'
    assert hashlib.sha256(body).hexdigest() == before['files'][n] == hashlib.sha256((checkout/n).read_bytes()).hexdigest()
assert state['environment']['GOMAXPROCS'] == '2' and state['environment']['GOMEMLIMIT'] == '512MiB'
assert state['environment']['WF_GRAPH_CONTINUATION_LIMIT_BUDGET'] == '64'
assert state['environment']['SIM_COVERAGE_SUMMARY'] == state['environment']['SIM_PROGRESS'] == '1'
expected = []
def compile_command(name, package, race):
    binary = root/(name+'.test')
    expected.append((['go', 'test', '-p=1']+(['-race'] if race else [])+['-c', '-o', str(binary), package], checkout, '1000'))
    info = state['binaries'][name]
    assert info['path'] == str(binary) and info['race'] == race
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == info['sha256']
    assert ('-race=true' in subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)) == race
def execution(name, package, selector, seeds='1000'):
    expected.append((['go', 'tool', 'test2json', '-t', '-p', 'js-wf/'+package, str(root/(name+'.test')),
        '-test.v=test2json', '-test.run='+selector, '-test.count=1', '-test.timeout=300m'], checkout/package, seeds))
compile_command('graph-race', './internal/graphpublication', True)
execution('graph-race', 'internal/graphpublication', '^TestGraphPrefixCompaction')
expected.append((['go', 'test', '-p=1', '-race', '-overlay='+str(root/'overlay.json'), './internal/graphpublication',
    '-run', '^TestGraphPrefixCompactionRechecksOriginalGrant$', '-count=1', '-v'], checkout, '1000'))
compile_command('worker-race', './worker', True)
execution('worker-race', 'worker', '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R1/archive=true$')
compile_command('sim-race', './sim', True)
execution('sim-race', 'sim', '^TestSeededGraphCheckpointCompactionReplay$')
compile_command('sim-normal', './sim', False)
execution('sim-normal', 'sim', '^TestPinnedRegressionCorpus$')
execution('sim-normal', 'sim', '^TestSeededGraphCheckpointCompactionReplay$', '100000')
assert len(state['commands']) == len(expected) == 10
for i, (row, (args, cwd, seeds)) in enumerate(zip(state['commands'], expected)):
    assert row['command'] == args and row['cwd'] == str(cwd) and row['seeds'] == seeds and row['finished']
    assert row['exit'] == (1 if i == 2 else 0)
def events(filename):
    rows = [json.loads(line) for line in (root/filename).read_text().splitlines()]
    assert not any(r['Action'] in ('fail', 'skip') or 'WARNING: DATA RACE' in r.get('Output', '') for r in rows)
    runs = Counter(r['Test'] for r in rows if r['Action'] == 'run' and 'Test' in r)
    passes = Counter(r['Test'] for r in rows if r['Action'] == 'pass' and 'Test' in r)
    assert runs == passes and all(v == 1 for v in runs.values())
    assert sum(r['Action'] == 'pass' and 'Test' not in r for r in rows) == 1
    return set(passes), ''.join(r.get('Output', '') for r in rows)
graph, _ = events('graph-events.jsonl')
roots = {n for n in graph if '/' not in n}
assert roots == {'TestGraphPrefixCompactionPreparationFailures', 'TestGraphPrefixCompactionPreservesStreamsAndOwnedReuse',
    'TestGraphPrefixCompactionSeededPinsAndCollection', 'TestGraphPrefixCompactionPublicationFaults', 'TestGraphPrefixCompactionRechecksOriginalGrant'}
negative = (root/'negative.log').read_text()
leaves = re.findall(r'--- FAIL: TestGraphPrefixCompactionRechecksOriginalGrant/(\S+) ', negative)
required = {a+'/'+b for a in ('prepare', 'commit') for b in ('missing', 'destination', 'location')}
assert len(leaves) == 6 and set(leaves) == required and 'build failed' not in negative and 'WARNING: DATA RACE' not in negative
owned = checkout/'internal/graphpublication/owned.go'
needle = 'func (p Protocol) verifyOwnedGrant(ctx context.Context, destination string, base Root, payload OwnedPayload) error {'
assert (root/'disabled-owned.go').read_text() == owned.read_text().replace(needle, needle+'\n return nil // Negative control: bypass original grant.', 1)
assert json.loads((root/'overlay.json').read_text()) == {'Replace': {str(owned): str(root/'disabled-owned.go')}}
native, output = events('native64-events.jsonl')
top = 'TestNativeGraphContinuationGlobalLimitAndTerminalSlot'
assert native == {top, top+'/R1/archive=true'}
assert 'GRAPH_CONTINUATION_LIMIT budget=64 entries=64 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=63 prefix_stage_calls=1/1 production_cap=false padding_operations=24' in output
for operation in ('continuation_checkpoint_publish', 'continuation_archive', 'continuation_publish'):
    assert len(re.findall('operation='+operation+' ', output)) == 2
pins, _ = events('pins-events.jsonl')
pin_names = {Path(n).name for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')}
assert len(pin_names) == 853 and pins == {'TestPinnedRegressionCorpus'} | {'TestPinnedRegressionCorpus/'+n for n in pin_names}
for filename, count in [('race1000-events.jsonl', 1000), ('normal100000-events.jsonl', 100000)]:
    tests, output = events(filename)
    assert tests == {'TestSeededGraphCheckpointCompactionReplay'}
    proof = f'TIER1_SEEDS test=TestSeededGraphCheckpointCompactionReplay first=1 last={count} completed={count} requested={count}'
    assert output.count(proof) == 1
result = dict(accepted=False, execution_evidence_verified=True, known_relocated_node_grant_validation_bug=True, source=state['source'], git_verified_inputs=len(names), actual_supervisor_exit=0,
    race_seed_bodies=1000, normal_seed_bodies=100000, saved_pins=853, required_negative_failures=6,
    bounded_native=dict(replicas=1, archive=True, budget=64, entries=64, checkpoints=2, terminal_slot=63, forbidden_effects=0),
    binaries={k:v['sha256'] for k,v in state['binaries'].items()},
    scope='Old frozen source has a confirmed relocated-node grant validation bug; successful commands do not qualify corrected grant safety. Frozen execution evidence only. Full latest source simulation suites, actual100000-entry gate, native fault/scale/soak/retention/import/admission/rollout remain open.')
(base/'qualification-review.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result, indent=2))
