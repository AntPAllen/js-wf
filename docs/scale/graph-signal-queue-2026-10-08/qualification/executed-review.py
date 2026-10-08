import hashlib,json,re,subprocess
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-signal-queue-2026-10-08/qualification'
before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert before==after
head=before['head']
def git(*args):return subprocess.check_output(['git',*args],cwd=repo)
assert head==git('rev-parse','HEAD').decode().strip()
for name,record in before['files'].items():
 data=git('show',head+':'+name)
 assert data==(repo/name).read_bytes()
 assert record==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
commands=json.loads((base/'commands.json').read_text());assert len(commands)==10
expected_names={n+'-'+mode for n in ('journal','client','worker','signal-worker','pins') for mode in ('normal','race')}
assert {c['name'] for c in commands}==expected_names
pins={Path(n).name for n in before['files'] if n.startswith('sim/testdata/regressions/') and n.endswith('.json')};assert len(pins)==710
summary=[]
for c in commands:
 data=(base/(c['name']+'.jsonl')).read_bytes()
 assert hashlib.sha256(data).hexdigest()==c['log_sha256']
 rows=[json.loads(line) for line in data.splitlines()]
 assert c['returncode']==0
 assert not any(r.get('Action') in ('fail','skip','build-fail') for r in rows)
 assert b'DATA RACE' not in data
 pattern=c['argv'][c['argv'].index('-run')+1]
 for package in c['expected']:
  names=set()
  for name in before['files']:
   if name.startswith(package.removeprefix('js-wf/')+'/') and name.endswith('_test.go') and name.count('/')==1:
    source=git('show',head+':'+name).decode()
    names.update(n for n in re.findall(r'^func (Test\w+)\(t \*testing.T\)',source,re.M) if re.match(pattern,n))
  actual={r['Test'] for r in rows if r.get('Package')==package and r.get('Action')=='pass' and r.get('Test') and '/' not in r['Test']}
  assert names==actual==set(c['expected'][package])
  terminal=[r for r in rows if r.get('Package')==package and not r.get('Test') and r.get('Action') in ('pass','fail')]
  assert len(terminal)==1 and terminal[0]['Action']=='pass'
 if c['name'].startswith('pins-'):
  actual={r['Test'].removeprefix('TestPinnedRegressionCorpus/') for r in rows if r.get('Action')=='pass' and r.get('Test','').startswith('TestPinnedRegressionCorpus/')}
  assert actual==pins
 if c['name'].startswith('client-'):
  prefix='TestNativeGraphCanonicalSignalPublicationAndDrain/R'
  for replicas in (1,3):
   name=prefix+str(replicas)
   assert any(r.get('Action')=='pass' and r.get('Test')==name for r in rows)
   assert any(r.get('Test')==name and 'zero physical objects/chunks' in r.get('Output','') for r in rows)
  cuts=[r for r in rows if r.get('Action')=='pass' and re.fullmatch(r'TestGraphCanonicalSignalPublicationRecoveryCuts/(healthy|source_drop|source_lost_ack|queue_drop|queue_lost_readback|enqueue_drop|enqueue_lost_ack)/\d+',r.get('Test',''))]
  assert len(cuts)==112
 if c['name'].startswith('signal-worker-'):
  for replicas in (1,3):
   name='TestNativeCanonicalSignalQueueWorkerReplay/R'+str(replicas)
   assert any(r.get('Action')=='pass' and r.get('Test')==name for r in rows)
   assert any(r.get('Test')==name and 'exactly two consumptions, one effect' in r.get('Output','') for r in rows)
  children={r['Test'] for r in rows if r.get('Action')=='pass' and re.fullmatch(r'TestNativeCanonicalSignalQueueChildTransferAndReplay/R[13]/async=(true|false)/external-result=(true|false)',r.get('Test',''))}
  assert len(children)==8
 summary.append(dict(name=c['name'],groups={p:len(n) for p,n in c['expected'].items()},seconds=c['wall_seconds']))
result=dict(source=head,selected_inputs=len(before['files']),pins=len(pins),commands=summary,verdict='PASS',scope='Canonical Signal publication, ordered queue binding, explicit recovery, production worker consumption/replay/cancellation/child transfer and selected compatibility checks. Autonomous catalog repair, shared whole-flow simulation, full extended simulation/native matrices, production online GC and original wider/release gates remain open.')
(base/'independent-review.json').write_text(json.dumps(result,indent=2)+'\n')
(base/'executed-review.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(result))
