"""Correct an empty corpus selector without repeating other qualifications."""
import datetime,hashlib,json,os,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-selected-child-20261010')
checkout=Path('/home/exedev/js-wf-selected-child-qualification')
state=json.loads((base/'state.json').read_text())
assert any(c.get('exit')==0 and c['command']==['go','test','-c','-o',str(root/'sim-normal.test'),'./sim'] for c in state['commands'])
original=[json.loads(line) for line in (root/'corpus-events.jsonl').read_text().splitlines()]
assert not any(r.get('Test') for r in original) and any('no tests to run' in r.get('Output','') for r in original)
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
binary=root/'sim-normal.test'
receipt=dict(source=state['source'],pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=stamp(),binary=str(binary),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),original_selector='^TestSavedSeedReplayCorpus$',original_selected_cases=0,commands=[])
def save():(base/'corpus-correction.json').write_text(json.dumps(receipt,indent=2)+'\n')
expected=sorted(p.stem for p in (checkout/'sim/testdata/regressions').glob('*.json'))
assert len(expected)==841
receipt['expected_cases']=expected
before=json.loads((root/'source-before.json').read_text())
assert all(hashlib.sha256((checkout/name).read_bytes()).hexdigest()==digest for name,digest in before['files'].items())
save()
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
def run(args,filename):
 row=dict(command=args,cwd=str(checkout/'sim'),started=stamp());receipt['commands'].append(row);save()
 with (root/filename).open('wb') as output:row['exit']=subprocess.call(args,cwd=checkout/'sim',env=env,stdout=output,stderr=subprocess.STDOUT)
 row['finished']=stamp();save();assert row['exit']==0
run([str(binary),'-test.list=^TestPinnedRegressionCorpus$'],'corpus-inventory.txt')
assert (root/'corpus-inventory.txt').read_text().strip()=='TestPinnedRegressionCorpus'
run(['go','tool','test2json','-t','-p','js-wf/sim',str(binary),'-test.v=test2json','-test.run=^TestPinnedRegressionCorpus$','-test.count=1','-test.timeout=10m'],'corpus-corrected-events.jsonl')
rows=[json.loads(line) for line in (root/'corpus-corrected-events.jsonl').read_text().splitlines()]
prefix='TestPinnedRegressionCorpus/'
actual=sorted(r['Test'][len(prefix):] for r in rows if r['Action']=='pass' and r.get('Test','').startswith(prefix))
assert actual==expected and not any(r['Action'] in ('fail','skip') for r in rows)
assert hashlib.sha256(binary.read_bytes()).hexdigest()==receipt['binary_sha256']
assert all(hashlib.sha256((checkout/name).read_bytes()).hexdigest()==digest for name,digest in before['files'].items())
receipt.update(finished=stamp(),exit=0,passed_cases=len(actual),source_unchanged=True)
save()
