import hashlib,json,pathlib,re,subprocess,sys
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-runtime-tier1-2026-10-08/qualification'
source='dd98e39c29c5e8689f06fdbdcca7e944b53c0963'
parent=subprocess.check_output(['git','rev-parse',source+'^'],text=True).strip()
manifest=json.loads((base.parent/'development/pin-inventory.json').read_text())
modes={p['mode'] for p in manifest}
assert len(modes)==18
seeded=subprocess.check_output(['go','run','-p=1','./scripts/tier1-seed-inventory'],text=True).splitlines()
assert len(seeded)==145 and 'TestSeededGraphSignalRuntimeReplay' in seeded
assert seeded==(base.parent/'development/seeded-inventory.txt').read_text().splitlines()
pins={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
assert len(pins)==728
new={pathlib.Path(p['path']).name for p in manifest}
assert len(new)==18 and new<=pins
for p in manifest:
 body=(root/p['path']).read_bytes()
 assert hashlib.sha256(body).hexdigest()==p['sha256']
 trace=json.loads(body)
 assert trace['workload']=='graph_canonical_signal_runtime'
 assert trace['seed']==p['seed'] and trace['decisions'][0]['chosen']==p['mode']
for name in pins-new:
 path='sim/testdata/regressions/'+name
 assert (root/path).read_bytes()==subprocess.check_output(['git','show',parent+':'+path]),path
summary=[]
requested=sys.argv[1:] or ['normal','race']
for mode in requested:
 before=json.loads((base/(mode+'-source-before.json')).read_text())
 after=json.loads((base/(mode+'-source-after.json')).read_text())
 assert before==after and before['source']==source
 for path,digest in before['inputs'].items():
  assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,path
  assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+path])).hexdigest()==digest,path
 commands=json.loads((base/(mode+'-commands.json')).read_text())
 assert commands['source']==source and [c['name'] for c in commands['commands']]==['family','pins']
 assert commands['environment']['SIM_SEEDS']=='1000'
 for c in commands['commands']:
  assert c['exit_code']==0
  assert ('-race' in c['command'])==(mode=='race')
  assert '-count=1' in c['command'] and '-p=1' in c['command']
  pattern='^TestSeededGraphSignalRuntimeReplay$' if c['name']=='family' else '^TestPinnedRegressionCorpus$'
  assert c['command'][c['command'].index('-run')+1]==pattern
  rows=[json.loads(l) for l in (base/(mode+'-'+c['name']+'.jsonl')).read_text().splitlines()]
  assert not any(r.get('Action') in ('fail','skip','build-fail') for r in rows)
  assert not any('WARNING: DATA RACE' in r.get('Output','') for r in rows)
  assert len([r for r in rows if r.get('Action')=='pass' and 'Test' not in r and r.get('Package')=='js-wf/sim'])==1
  passed={r['Test'] for r in rows if r.get('Action')=='pass' and 'Test' in r}
  if c['name']=='family':
   assert passed=={'TestSeededGraphSignalRuntimeReplay'}
   output=''.join(r.get('Output','') for r in rows)
   assert 'TIER1_SEEDS test=TestSeededGraphSignalRuntimeReplay first=1 last=1000 completed=1000 requested=1000' in output
   found=re.search(r'canonical Signal runtime modes=map\[([^\]]+)\]',output)
   assert found
   counts={name:int(count) for name,count in (part.split(':') for part in found[1].split())}
   assert set(counts)==modes and sum(counts.values())==1000 and min(counts.values())>0
   summary.append(dict(mode=mode,mode_counts=counts,**c))
  else:
   assert {n.split('/',1)[1] for n in passed if n.startswith('TestPinnedRegressionCorpus/')}==pins
   summary.append(dict(mode=mode,**c))
result={'source':source,'selected_inputs':len(before['inputs']),'seeded_family_inventory':145,'seeds_per_family_per_mode':1000,'new_modes':len(modes),'pins_per_mode':len(pins),'unchanged_parent_pins':710,'commands':summary,'verdict':'PASS','scope':'Shared canonical Signal runtime family and complete pinned corpus in listed modes. Complete 145-family suite/extended campaigns and original wider implementation/acceptance gates remain separate.'}
(base/('review.json' if len(requested)==2 else requested[0]+'-review.json')).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
