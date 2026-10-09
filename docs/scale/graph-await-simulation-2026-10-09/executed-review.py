"""Review focused simulation coverage, source stability and preserved corpus."""
import hashlib,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent;repo=base.parents[2]
before=json.loads((base/'source-before.json').read_text())
for p,h in before['files'].items():assert hashlib.sha256((repo/p).read_bytes()).hexdigest()==h,p
old=json.loads((base/'previous-pins.json').read_text());assert len(old)==762
for name,h in old.items():
 p=repo/'sim/testdata/regressions'/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==h,name
 committed=subprocess.check_output(['git','show',before['revision']+':sim/testdata/regressions/'+name],cwd=repo)
 assert hashlib.sha256(committed).hexdigest()==h,name
pins={p.name for p in (repo/'sim/testdata/regressions').glob('*.json')}
assert len(pins)==786
new=pins-set(old);assert len(new)==24
modes=set()
for name in new:
 p=repo/'sim/testdata/regressions'/name
 assert p.read_bytes()==(base/'generated'/name).read_bytes()
 trace=json.loads(p.read_text());assert trace['workload']=='graph_await_contention'
 modes.add(trace['decisions'][0]['chosen'])
expected={f'v{v}_{m}' for v in [4,5,6] for m in ['observe','pinned','release','generation','purge','cancel','unknown','corrupt']}
assert modes==expected
inventory=(base/'seed-body-inventory.txt').read_text().splitlines()
assert len(inventory)==150 and len(set(inventory))==150
assert 'TestSeededGraphAwaitContentionReplay' in inventory
logs={}
for kind in ['normal','race']:
 assert json.loads((base/(kind+'.exit.json')).read_text())['exit_code']==0
 p=base/('qualified-'+kind+'.log');s=p.read_text()
 assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:',s,re.M)
 assert 'TIER1_SEEDS test=TestSeededGraphAwaitContentionReplay first=1 last=1000 completed=1000 requested=1000' in s
 for name in pins:assert re.search(r'^\s*--- PASS: TestPinnedRegressionCorpus/'+re.escape(name)+r' \(',s,re.M),(kind,name)
 for package in ['js-wf/sim','js-wf/client']:assert re.search(r'^ok\s+'+re.escape(package)+r'\s+[\d.]+s$',s,re.M),(kind,package)
 assert '--- PASS: TestGraphAwaitOwnReadDeadline/deadline-corrupt' in s
 counts={m:int(n) for m,n in re.findall(r'(v[456]_[a-z]+):(\d+)',s)}
 assert set(counts)==expected and sum(counts.values())==1000
 logs[kind]={'combination_counts':counts,'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'times':re.findall(r'^ok\s+(\S+)\s+([\d.]+s)$',s,re.M)}
baseline=json.loads((base/'baseline-source.json').read_text())
data=subprocess.check_output(['git','show',baseline['revision']+':'+baseline['path']],cwd=repo)
assert data==(base/'baseline-graph-journal.go.txt').read_bytes()
assert hashlib.sha256(data).hexdigest()==baseline['sha256']
assert json.loads((base/'baseline-failure.exit.json').read_text())['exit_code']==1
s=(base/'baseline-failure.log').read_text();assert 'FAULT_SEED=2' in s and 'generation transferred/returned result' in s and 'blob publication CAS conflict' in s
assert json.loads((base/'baseline-minimize.exit.json').read_text())['exit_code']==0
assert 'in 4 reproductions' in (base/'baseline-minimize.log').read_text()
assert (base/'baseline-minimized.json').is_file()
review={'verdict':'PASS focused normal/race Await family and whole saved corpus','repository_inputs_unchanged':len(before['files']),'seeded_inventory':150,'accepted_family':'TestSeededGraphAwaitContentionReplay','completed_seed_bodies_per_campaign':1000,'coverage_combinations':24,'saved_corpus':786,'new_saved_traces':24,'old_pins_byte_identical':762,'old_code_failure_seed':2,'old_code_failure_test_seconds':0.009,'minimizer_reproductions':4,'logs':logs,'scope':'One shared family and all saved regressions plus directed/timer controls. No full 150-family, extended or native worker liveness acceptance. Frozen fca8264 full race remains independent and excludes later production changes.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
(base/'source-after.json').write_text(json.dumps(before,indent=2)+'\n')
print(json.dumps(review))
