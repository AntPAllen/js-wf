"""Independent source, binary, seed-body, corpus and negative-control review."""
import hashlib,io,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent;s=json.loads((base/'state.json').read_text());root=Path(s['root']);checkout=Path(s['checkout'])
assert s.get('phase')=='closed' and s['exit']==0
props=subprocess.check_output(['systemctl','--user','show','js-wf-graph-replay-lifecycle-20261010.service','-p','LoadState','-p','MainPID','-p','ExecMainStatus','-p','InvocationID'],text=True)
assert 'LoadState=loaded\n' in props and 'MainPID=0\n' in props and 'ExecMainStatus=0\n' in props and 'InvocationID='+s['invocation']+'\n' in props
(root/'supervisor-exit.txt').write_text(props)
def hash(p):return hashlib.sha256(p.read_bytes()).hexdigest()
before,after=[json.loads((root/n).read_text()) for n in ('source-before.json','source-after.json')];assert before==after and before['source']==s['source']
names=subprocess.check_output(['git','ls-tree','-r','--name-only',s['source']],cwd=checkout,text=True).splitlines();names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(names)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=checkout,input=''.join(s['source']+':'+n+'\n' for n in names).encode()))
for n in names:
 header=stream.readline().decode().split();assert header[1]=='blob';body=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==hash(checkout/n)
assert len(s['commands'])==6 and [r['exit'] for r in s['commands']]==[0,0,0,0,1,0] and all(r['finished'] for r in s['commands'])
assert s['commands'][2]['seeds']=='1000' and s['commands'][5]['seeds']=='100000'
assert '-test.run=^TestSeededGraphReplayLifecycleReplay$' in s['commands'][2]['command'] and '-test.run=^TestSeededGraphReplayLifecycleReplay$' in s['commands'][5]['command']
assert '-test.run=^TestPinnedRegressionCorpus$' in s['commands'][3]['command']
assert set(s['binaries'])=={'sim-race.test','sim-normal.test'}
for n,receipt in s['binaries'].items():
 p=root/n;assert hash(p)==receipt['sha256'] and p.stat().st_size==receipt['bytes'];assert ('-race=true' in subprocess.check_output(['go','version','-m',str(p)],text=True))==(n=='sim-race.test')
def events(n):
 rows=[json.loads(l) for l in (root/n).read_text().splitlines()];assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows);assert sum(r['Action']=='pass' and 'Test' not in r for r in rows)==1;return rows
modes={'healthy','before_pin_retire','before_pin_replace','after_pin_retire','after_pin_replace'}
for file,seeds in (('race-events.jsonl',1000),('normal100000-events.jsonl',100000)):
 rows=events(file);assert {r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}=={'TestSeededGraphReplayLifecycleReplay'}
 text=''.join(r.get('Output','') for r in rows)
 assert 'TIER1_SEEDS test=TestSeededGraphReplayLifecycleReplay first=1 last='+str(seeds)+' completed='+str(seeds)+' requested='+str(seeds) in text
 summary=re.search(r'GRAPH_REPLAY_LIFECYCLE modes=(\{[^\n]+\}) exact_replay=true',text);assert summary
 counts=json.loads(summary[1]);assert set(counts)==modes and sum(counts.values())==seeds and min(counts.values())>0
 dimensions=re.search(r'GRAPH_REPLAY_LIFECYCLE dimensions=(\{[^\n]+\})',text);assert dimensions
 counts=json.loads(dimensions[1]);assert set(counts)=={m+'/'+z for m in modes for z in ('small','large')} and sum(counts.values())==seeds and min(counts.values())>0

corpus=[Path(n).name for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')];assert len(corpus)==853
rows=events('corpus-events.jsonl');assert sorted(r['Test'].split('/',1)[1] for r in rows if r['Action']=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/'))==sorted(corpus)
negative=(root/'negative.log').read_text();failed={m for m in re.findall(r'^    --- FAIL: TestPinnedRegressionCorpus/(\S+) ',negative,re.M)};assert failed=={'graph-replay-lifecycle-'+n+'.json' for n in modes-{'healthy'}}
assert 'build failed' not in negative
p=checkout/'journal/graph.go';body=p.read_text();start=body.index('func (s *GraphStore) open(');needle='if c == nil || c.Invocation != invocation || c.Retired || c.Purging {'
assert json.loads((root/'overlay.json').read_text())=={'Replace':{str(p):str(root/'disabled.go')}};assert (root/'disabled.go').read_text()==body[:start]+body[start:].replace(needle,'if c == nil { // required generation-fence negative control',1)

result=dict(accepted=True,source=s['source'],git_verified_inputs=len(names),actual_supervisor_exit=0,supervisor_exit_proven=True,race_seed_bodies=1000,normal_seed_bodies=100000,lifecycle_modes=5,input_size_mode_cells=10,saved_traces=853,new_pins=5,required_negative_failures=4,scope='Production pinned export and generation admission across retirement/replacement, exact seeded trace replay. Native lifecycle and full latest158 suite, remaining extended/fault/retention/import/admission/rollout and broader original gates remain open.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
