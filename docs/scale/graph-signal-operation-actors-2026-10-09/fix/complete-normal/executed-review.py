from pathlib import Path
import subprocess,json,hashlib,re
base=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-signal-actors-qualified-20261009')
repo=Path('/home/exedev/js-wf-signal-actors-qualification')
source='cc8363da0f99dafbaff6eec454b85cc00fa62a63'
before=json.loads((root/'source-before.json').read_text())
assert before['revision']==source
for name,digest in before['files'].items():
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest,name
 assert hashlib.sha256(subprocess.check_output(['git','show',f'{source}:{name}'],cwd=repo)).hexdigest()==digest,name
binary=json.loads((root/'normal-binary.json').read_text())
assert hashlib.sha256((root/'normal.test').read_bytes()).hexdigest()==binary['sha256']
assert '-race=true' not in binary['build_info']
rows=json.loads((root/'command-results.json').read_text())
normal=[r for r in rows if r['log'] in ['normal-compile.log','normal-events.jsonl','normal-pin.log']]
assert len(normal)==3 and all(r['exit_code']==0 for r in normal)
events=[json.loads(x) for x in (root/'normal-events.jsonl').read_text().splitlines()]
passes={e.get('Test') for e in events if e['Action']=='pass'}
assert {'TestSeededGraphSignalOperationActorsReplay','TestGraphSignalCallerRetryAfterContention',None}<=passes
output=''.join(e.get('Output','') for e in events)
assert 'TIER1_SEEDS test=TestSeededGraphSignalOperationActorsReplay first=1 last=1000 completed=1000 requested=1000' in output
assert not any(e['Action']=='fail' for e in events)
pin=(root/'normal-pin.log').read_text()
assert '--- PASS: TestPinnedRegressionCorpus/graph-signal-actors-caller-retry.json' in pin
trace_data={}
for p in sorted((root/'pins').glob('*.json')):
 d=json.loads(p.read_text());assert d['workload']=='graph_signal_operation_actors'
 trace_data[p.name]={'seed':d['seed'],'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
assert len(trace_data)==6
result={'source':source,'focused_normal_completed_bodies':1000,'focused_normal_exact_replays':1000,'directed_contention_replay':True,'new_registered_pin_passed':1,'source_inputs_git_matched_and_still_unchanged':len(before['files']),'package_elapsed':events[-1]['Elapsed'],'captures':trace_data,'scope':'focused family/directive/new pin only; not complete 152-family suite/all811 pins','race_acceptance':'pending'}
(base/'review-result.json').write_text(json.dumps(result,indent=2)+'\n')
(base/'normal-source-observed-after.json').write_text(json.dumps({'revision':source,'files':before['files'],'scope':'source observed unchanged after normal termination while separate race job is live'},indent=2)+'\n')
print(json.dumps(result))
