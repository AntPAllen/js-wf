"""Freeze and qualify canonical Signal publication/binding/worker intake and compatibility."""
import hashlib, json, os, re, subprocess, time
from pathlib import Path
repo=Path(__file__).resolve().parents[3]
base=Path(__file__).resolve().parent/'qualification'
base.mkdir(exist_ok=False)
def git(*args):return subprocess.check_output(['git',*args],cwd=repo)
def write(name,obj):(base/name).write_text(json.dumps(obj,indent=2)+'\n')
head=git('rev-parse','HEAD').decode().strip()
files=[p for p in git('ls-files','-z').decode().split('\0') if p and (p.endswith('.go') or p in ('go.mod','go.sum','.github/workflows/graph-publication.yml') or p.startswith('sim/testdata/regressions/'))]
def inventory():
 out={}
 for name in files:
  data=(repo/name).read_bytes()
  assert data==git('show',head+':'+name),name
  out[name]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
 return out
before=inventory();write('source-before.json',dict(head=head,files=before))
specs=[
 ('journal',['./journal'],r'^(TestGraphJournal|TestNativeGraphJournal|TestGraphCanonicalStart|TestGraphCanonicalSignalInputs|TestNativeGraphCanonicalSignalInputs|TestGraphCanonicalSignalQueue)'),
 ('client',['./client'],r'^Test'),
 ('worker',['./worker'],r'^(TestNativeGraphWorkerReplayInputsSignalsAndResults|TestNativeGraphChildResultTransferAndReplay|TestGraphChildReplayRejectsForgedProvenance|TestGraphSelectedChildRequiresExactRecordedSignal|TestNativeGraphWorkerParentNotifications|TestNativeCanonicalStartRecoveryWorkerReplayAndPurge)$'),
 ('signal-worker',['./worker'],r'^TestNativeCanonicalSignalQueue'),
 ('pins',['./sim'],r'^TestPinnedRegressionCorpus$')]

commands=[]
for mode in ('normal','race'):
 for name,packages,pattern in specs:
  expected={}
  for package in packages:
   names=set()
   for p in (repo/package[2:]).glob('*_test.go'):
    names.update(n for n in re.findall(r'^func (Test\w+)\(t \*testing.T\)',p.read_text(),re.M) if re.match(pattern,n))
   assert names
   expected['js-wf/'+package[2:]]=sorted(names)
  argv=['go','test','-json','-p=1',*(['-race'] if mode=='race' else []),*packages,'-run',pattern,'-count=1','-timeout=5m']
  log=base/(name+'-'+mode+'.jsonl')
  start=time.time()
  with log.open('wb') as stream:
   result=subprocess.run(argv,cwd=repo,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=stream,stderr=subprocess.STDOUT)
  rows=[json.loads(line) for line in log.read_text().splitlines()]
  actual={package:sorted({r['Test'] for r in rows if r.get('Package')==package and r.get('Action')=='pass' and r.get('Test') and '/' not in r['Test']}) for package in expected}
  terminal=[r for r in rows if r.get('Action') in ('pass','fail') and not r.get('Test')]
  record=dict(name=name+'-'+mode,argv=argv,returncode=result.returncode,wall_seconds=time.time()-start,expected=expected,actual=actual,terminal=terminal,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
  commands.append(record);write('commands.json',commands)
  assert result.returncode==0 and expected==actual and not any(r.get('Action') in ('fail','skip','build-fail') for r in rows),record
  assert {r['Package'] for r in terminal}==set(expected) and all(r['Action']=='pass' for r in terminal)
  print(record['name']+' PASS '+str(round(record['wall_seconds'],3))+'s',flush=True)
after=inventory();write('source-after.json',dict(head=head,files=after))
assert before==after and head==git('rev-parse','HEAD').decode().strip()
write('review.json',dict(source=head,commands=len(commands),all_selected_groups_pass=True,all_selected_inputs_match_git_and_unchanged=True,input_count=len(before),scope='Canonical Signal publication, ordered binding, explicit recovery, production worker intake and selected compatibility controls. Autonomous catalog repair, shared whole-flow simulation and original wider gates remain open.'))
