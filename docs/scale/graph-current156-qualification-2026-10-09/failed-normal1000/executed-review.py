import hashlib,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[3]
source='fd95754a924c66fa2734d486356e0ce04d82dc01'
before=json.loads((base/'source-before.json').read_text())
assert before==json.loads((base/'source-after.json').read_text()) and before['revision']==source
process=subprocess.Popen(['git','cat-file','--batch'],cwd=repo,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
try:
 for name,digest in before['files'].items():
  process.stdin.write((source+':'+name+'\n').encode());process.stdin.flush()
  head=process.stdout.readline().split();assert len(head)==3 and head[1]==b'blob'
  raw=process.stdout.read(int(head[2]));assert process.stdout.read(1)==b'\n'
  assert hashlib.sha256(raw).hexdigest()==digest,name
finally:
 process.stdin.close();assert process.wait()==0
assert json.loads((base/'supervisor-launch-exit.json').read_text())['actual_exit_code']==1
binary=json.loads((base/'binary.json').read_text())
assert not binary['race_instrumented']
assert hashlib.sha256(Path('/home/exedev/js-wf-tier1-full156-normal1000-20261009/sim.test').read_bytes()).hexdigest()==binary['binary_sha256']
rows=[json.loads(line) for line in (base/'tier1-events.jsonl').open()]
failed=[r.get('Test','PACKAGE') for r in rows if r['Action']=='fail']
pin_failed=[name for name in failed if name.startswith('TestPinnedRegressionCorpus/')]
pin_passed=[r['Test'] for r in rows if r['Action']=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/')]
assert len(pin_failed)==110 and len(pin_passed)==731
assert set(failed)-set(pin_failed)=={'TestGraphSignalCallerRetryAfterContention','TestPinnedRegressionCorpus','PACKAGE'}
expected=set((base/'tier1-seeded-inventory.txt').read_text().splitlines())
proof={}
for row in rows:
 for test,first,last,completed,requested in re.findall(r'TIER1_SEEDS test=(\S+) first=(\d+) last=(\d+) completed=(\d+) requested=(\d+)',row.get('Output','')):
  assert test not in proof
  assert (first,last,completed,requested)==('1','1000','1000','1000'),test
  proof[test]=1000
assert set(proof)==expected and len(proof)==156
terminal=[r for r in rows if r['Action']=='fail' and 'Test' not in r]
assert len(terminal)==1 and terminal[0]['Elapsed']==3336.913
review=dict(verdict='FAIL frozen156 normal default suite',source=source,actual_supervisor_exit=1,elapsed_seconds=3336.913,source_inputs_git_verified_unchanged=len(before['files']),complete_seeded_families=156,completed_seed_bodies=sum(proof.values()),pins_passed=731,pins_failed=110,failed_directed_control='TestGraphSignalCallerRetryAfterContention',events_sha256=hashlib.sha256((base/'tier1-events.jsonl').read_bytes()).hexdigest(),scope='All seeded bodies completed, but default suite remains failed. This source predates trace migration, explicit caller-retry fault and slow-acquisition renewal fix; no current/default/native/extended or broader acceptance.',failed_pins=pin_failed)
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps({k:v for k,v in review.items() if k!='failed_pins'}))
