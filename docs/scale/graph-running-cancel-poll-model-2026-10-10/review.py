"""Independent source, binary, seed-body, corpus and negative-control review."""
import hashlib,io,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent;s=json.loads((base/'state.json').read_text());root=Path(s['root']);checkout=Path(s['checkout'])
assert s.get('phase')=='closed' and s['exit']==0
props=subprocess.check_output(['systemctl','--user','show','js-wf-graph-cancel-poll-20261010.service','-p','LoadState','-p','MainPID','-p','ExecMainStatus','-p','InvocationID'],text=True)
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
assert '-test.run=^TestSeededGraphRunningCancelPollReplay$' in s['commands'][2]['command'] and '-test.run=^TestSeededGraphRunningCancelPollReplay$' in s['commands'][5]['command']
assert '-test.run=^TestPinnedRegressionCorpus$' in s['commands'][3]['command']
assert set(s['binaries'])=={'sim-race.test','sim-normal.test'}
for n,receipt in s['binaries'].items():
 p=root/n;assert hash(p)==receipt['sha256'] and p.stat().st_size==receipt['bytes'];assert ('-race=true' in subprocess.check_output(['go','version','-m',str(p)],text=True))==(n=='sim-race.test')
def events(n):
 rows=[json.loads(l) for l in (root/n).read_text().splitlines()];assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows);assert sum(r['Action']=='pass' and 'Test' not in r for r in rows)==1;return rows
modes={'absent','reserved','bound_source_purged','foreign_key','foreign_generation','root_unknown','owned_read_unknown'}
for file,seeds in (('race-events.jsonl',1000),('normal100000-events.jsonl',100000)):
 rows=events(file);assert {r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}=={'TestSeededGraphRunningCancelPollReplay'}
 text=''.join(r.get('Output','') for r in rows)
 assert 'TIER1_SEEDS test=TestSeededGraphRunningCancelPollReplay first=1 last='+str(seeds)+' completed='+str(seeds)+' requested='+str(seeds) in text
 summary=re.search(r'canonical cancellation poll modes=map\[([^\]]+)\]',text);assert summary
 counts={k:int(v) for k,v in re.findall(r'(\w+):(\d+)',summary[1])};assert set(counts)==modes and sum(counts.values())==seeds and min(counts.values())>0
corpus=[Path(n).name for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')];assert len(corpus)==848
rows=events('corpus-events.jsonl');assert sorted(r['Test'].split('/',1)[1] for r in rows if r['Action']=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/'))==sorted(corpus)
negative=(root/'negative.log').read_text();failed={m for m in re.findall(r'^    --- FAIL: TestPinnedRegressionCorpus/(\S+) ',negative,re.M)};assert failed=={'graph-running-cancel-poll-'+n+'.json' for n in ('bound_source_purged','root_unknown','owned_read_unknown')}
p=checkout/'worker/worker.go';needle='func (w *Worker) pollRunningCancellation(ctx context.Context, typ, id, generation string, expected *cancelWaiter) (bool, error) {\n\tif w.graphJournal != nil && w.graphJournal.CanonicalSignals() {'
assert json.loads((root/'overlay.json').read_text())=={'Replace':{str(p):str(root/'disabled.go')}};assert (root/'disabled.go').read_text()==p.read_text().replace(needle,needle.replace('if w.graphJournal','if false && w.graphJournal'))
result=dict(accepted=True,source=s['source'],git_verified_inputs=len(names),actual_supervisor_exit=0,supervisor_exit_proven=True,race_seed_bodies=1000,normal_seed_bodies=100000,poll_modes=7,saved_traces=848,new_pins=7,required_negative_failures=3,scope='Production canonical cancellation polling decisions and exact seeded trace replay; native restored effects qualified separately. Full latest157 suite, remaining extended/fault/retention/import/admission/rollout and broader original gates remain open.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
