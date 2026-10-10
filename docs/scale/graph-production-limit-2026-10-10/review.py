"""Review a closed actual graph cap run; live or missing handles prove nothing."""
import datetime,hashlib,io,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'state.json').read_text())
root,checkout=Path(state['root']),Path(state['checkout'])
assert state['phase']=='closed' and state['finished'] and state['exit']==0
properties=subprocess.check_output(['systemctl','--user','show','js-wf-graph-production-limit-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','ActiveState','-p','InvocationID'],text=True)
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
assert state['environment']['WF_GRAPH_CONTINUATION_LIMIT_BUDGET']=='100000'
assert len(state['commands'])==2 and all(c['exit']==0 and c['finished'] for c in state['commands'])
binary=root/'worker-race.test'
assert state['commands'][0]['command']==['go','test','-race','-c','-o',str(binary),'./worker']
assert state['commands'][1]['cwd']==str(checkout/'worker')
assert state['commands'][1]['command']==['go','tool','test2json','-t','-p','js-wf/worker',str(binary),'-test.v=test2json','-test.run=^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R1/archive=true$','-test.count=1','-test.timeout=300m']
assert binary.stat().st_size==state['binary']['bytes']
assert hashlib.sha256(binary.read_bytes()).hexdigest()==state['binary']['sha256']
assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
rows=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows)
top='TestNativeGraphContinuationGlobalLimitAndTerminalSlot'
assert {r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}=={top,top+'/R1/archive=true'}
package=[r for r in rows if r['Action']=='pass' and 'Test' not in r];assert len(package)==1
output=''.join(r.get('Output','') for r in rows)
summary='GRAPH_CONTINUATION_LIMIT budget=100000 entries=100000 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=99999 prefix_stage_calls=1/1 production_cap=true padding_operations=49992'
assert output.count(summary)==1
progress=[int(n) for n in re.findall(r'GRAPH_LIMIT_PADDING completed=(\d+) requested=49992 budget=100000',output)]
assert progress==sorted(list(range(1000,49001,1000))+[24996,49992])
# Bind the expected cap/default branch and real SDK padding to the executed source.
fixture=(checkout/'worker/graph_continuation_limit_test.go').read_text()
assert 'padding := int((budget - 16) / 2)' in fixture
assert 'if budget != journal.MaxEntries {\n\t\t\t\t\t\tw.maxEntries = budget' in fixture
assert 'c.SetState("padding", i)' in fixture
result=dict(accepted=True,source=state['source'],git_verified_inputs=len(names),actual_go_exit=0,actual_supervisor_exit=0,cases=['R1/archive=true'],budget=100000,entries=100000,terminal_slot=99999,checkpoints=2,padding_operations=49992,forbidden_effects=0,prefix_stage_calls=[1,1],production_cap_unchanged=True,binary_sha256=state['binary']['sha256'],events_sha256=hashlib.sha256((root/'events.jsonl').read_bytes()).hexdigest(),elapsed=package[0]['Elapsed'],reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Actual production graph cap R1/archive only. Other replicas/archive modes, collection, faults, public admission and broader original gates remain separate.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
