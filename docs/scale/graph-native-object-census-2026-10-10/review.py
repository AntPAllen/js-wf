"""Review a closed bounded diagnostic; it does not qualify the production cap."""
import datetime,hashlib,io,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'state.json').read_text())
root,checkout=Path(state['root']),Path(state['checkout'])
assert state['phase']=='closed' and state['finished'] and state['exit']==0
properties=subprocess.check_output(['systemctl','--user','show','js-wf-graph-native-object-census-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','ActiveState','-p','InvocationID','-p','LoadState'],text=True)
assert 'LoadState=loaded\n' in properties
assert 'MainPID=0\n' in properties and 'ExecMainStatus=0\n' in properties
assert 'InvocationID='+state['invocation']+'\n' in properties
(root/'supervisor-exit.txt').write_text(properties)
before,after=[json.loads((root/n).read_text()) for n in ('source-before.json','source-after.json')]
assert before==after and before['source']==state['source']
tracked=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=checkout,text=True).splitlines()
names=[n for n in tracked if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(names)==set(before['files'])
raw=subprocess.check_output(['git','cat-file','--batch'],cwd=checkout,input=''.join(state['source']+':'+n+'\n' for n in names).encode())
stream=io.BytesIO(raw)
for name in names:
 header=stream.readline().decode().split();assert header[1]=='blob'
 body=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(body).hexdigest()==before['files'][name]
 assert hashlib.sha256((checkout/name).read_bytes()).hexdigest()==before['files'][name]
assert state['environment']['WF_GRAPH_CONTINUATION_LIMIT_BUDGET']=='64'
assert state['environment']['WF_GRAPH_LIMIT_PORT_PROFILE']=='1'
assert len(state['commands'])==5 and [c['exit'] for c in state['commands']]==[0,0,0,0,1] and all(c['finished'] for c in state['commands'])
binary=root/'worker-race.test'
assert state['commands'][0]['command']==['go','test','-race','-c','-o',str(binary),'./worker']
assert state['commands'][1]['cwd']==str(checkout/'worker')
assert state['commands'][1]['command']==['go','tool','test2json','-t','-p','js-wf/worker',str(binary),'-test.v=test2json','-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R1/archive=true$','-test.count=1','-test.timeout=5m']
assert binary.stat().st_size==state['binary']['bytes']
assert hashlib.sha256(binary.read_bytes()).hexdigest()==state['binary']['sha256']
assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
rows=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows)
top='TestNativeGraphContinuationGlobalLimitAndTerminalSlot'
assert {r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}=={top,top+'/R1/archive=true'}
package=[r for r in rows if r['Action']=='pass' and 'Test' not in r];assert len(package)==1
output=''.join(r.get('Output','') for r in rows)
summary='GRAPH_CONTINUATION_LIMIT budget=64 entries=64 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=63 prefix_stage_calls=1/1 production_cap=false padding_operations=24'
assert output.count(summary)==1
progress=[int(n) for n in re.findall(r'GRAPH_LIMIT_PADDING completed=(\d+) requested=24 budget=64',output)]
assert progress==[12,24]
profiles=[]
for a,b,wall,ops in re.findall(r'GRAPH_LIMIT_PORT_PROFILE from=(\d+) to=(\d+) wall_ns=(\d+) operations=(\{[^\n]+\})', output):
 operations=json.loads(ops)
 assert set(operations)=={'ReadRoot','CASRoot','ReadBlob','CASBlob','Put','Get'}
 assert all(t['calls']>0 and t['total_ns']>0 and t['errors']==0 for t in operations.values())
 profiles.append(dict(from_operation=int(a),to_operation=int(b),wall_ns=int(wall),operations=operations))
assert [(p['from_operation'],p['to_operation']) for p in profiles]==[(0,12),(12,24)]
# Bind the expected cap/default branch and real SDK padding to the executed source.
fixture=(checkout/'worker/graph_continuation_limit_test.go').read_text()
assert 'padding := int((budget - 16) / 2)' in fixture
assert 'if budget != journal.MaxEntries {\n\t\t\t\t\t\tw.maxEntries = budget' in fixture
assert 'c.SetState("padding", i)' in fixture
graph_binary=root/'graph-race.test';receipt=state['graph_binary'];assert graph_binary.stat().st_size==receipt['bytes'] and hashlib.sha256(graph_binary.read_bytes()).hexdigest()==receipt['sha256'];assert '-race=true' in subprocess.check_output(['go','version','-m',str(graph_binary)],text=True)
assert state['commands'][2]['command']==['go','test','-race','-c','-o',str(graph_binary),'./internal/graphpublication']
assert state['commands'][3]['command']==['go','tool','test2json','-t','-p','js-wf/internal/graphpublication',str(graph_binary),'-test.v=test2json','-test.run=^TestNativeGraphObject','-test.count=1','-test.timeout=5m']
native=[json.loads(l) for l in (root/'native-events.jsonl').read_text().splitlines()];assert not any(r['Action']=='fail' or 'WARNING: DATA RACE' in r.get('Output','') for r in native)
assert sum(r['Action']=='pass' and 'Test' not in r for r in native)==1
native_roots={r['Test'] for r in native if r['Action']=='pass' and 'Test' in r and '/' not in r['Test']}
expected_roots=set()
for p in (checkout/'internal/graphpublication').glob('*_test.go'):expected_roots.update(re.findall(r'^func (TestNativeGraphObject\w+)\(',p.read_text(),re.M))
assert len(expected_roots)==7 and native_roots==expected_roots
expected_cells={'TestNativeGraphObjectGetFreshCombinedCensus/'+r+'/'+m for r in ('R1','R3') for m in ('healthy','no_rollup','allow_direct','sealed','wrong_format','foreign_subject','missing_chunks','unknown_info')}
assert {r['Test'] for r in native if r['Action']=='pass' and r.get('Test','').count('/')==2 and r['Test'].startswith('TestNativeGraphObjectGetFreshCombinedCensus/')}==expected_cells
negative=(root/'negative.log').read_text();failed=re.findall(r'^        --- FAIL: TestNativeGraphObjectGetFreshCombinedCensus/(\S+) ',negative,re.M)
assert len(failed)==8 and set(failed)=={r+'/'+m for r in ('R1','R3') for m in ('no_rollup','allow_direct','sealed','wrong_format')}
assert 'build failed' not in negative and 'WARNING: DATA RACE' not in negative
p=checkout/'internal/graphpublication/nats_objects.go';needle='if err = p.validateObjectConfig(census.Config); err != nil {'
assert p.read_text().count(needle)==1 and (root/'disabled.go').read_text()==p.read_text().replace(needle,'if false { // required unsafe-configuration negative control',1)
assert json.loads((root/'overlay.json').read_text())=={'Replace':{str(p):str(root/'disabled.go')}}
assert state['commands'][4]['command']==['go','test','-race','-overlay='+str(root/'overlay.json'),'./internal/graphpublication','-run','^TestNativeGraphObjectGetFreshCombinedCensus$','-count=1','-v','-timeout=5m']
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(names),actual_go_exit=0,actual_supervisor_exit=0,cases=['R1/archive=true'],budget=64,entries=64,terminal_slot=63,checkpoints=2,padding_operations=24,forbidden_effects=0,prefix_stage_calls=[1,1],production_cap_unchanged=True,binary_sha256=state['binary']['sha256'],events_sha256=hashlib.sha256((root/'events.jsonl').read_bytes()).hexdigest(),elapsed=package[0]['Elapsed'],reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),native_object_roots=sorted(native_roots),fresh_config_census_cells=16,required_unsafe_config_failures=8,graph_binary_sha256=receipt['sha256'],port_profiles=profiles,scope='Native object single fresh Info admission/census, all seven race roots/R1/R3 controls/eight required unsafe config failures, plus bounded 64-entry native R1/archive race diagnostic. Completed port calls include concurrent polling; summed durations overlap. This is not the 100000-entry gate or evidence of a NATS cause. Original live cap job unchanged; all broader gates remain open.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
