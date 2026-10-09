from pathlib import Path
import subprocess,json,hashlib,os,datetime,re
base=Path(__file__).resolve().parent
repo=Path('/home/exedev/js-wf-signal-actors-qualification')
root=Path('/home/exedev/js-wf-signal-actors-qualified-20261009')
revision='cc8363da0f99dafbaff6eec454b85cc00fa62a63'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==revision
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
assert not root.exists()
root.mkdir();(root/'pins').mkdir()
names=[n for n in subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines() if (n.endswith('.go') and not n.startswith('docs/')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def snapshot():return {n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names}
before=snapshot();(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
record={'source':revision,'working_directory':str(repo),'root':str(root),'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'focused 152nd family x1000 + directed contention regression; normal must pass before race starts; not complete suite/all pins','commands':[],'terminal_acceptance':False}
records=[]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_PROGRESS='1',SIM_COVERAGE_SUMMARY='1',SIM_SEEDS='1000',SIM_GRAPH_SIGNAL_ACTORS_ROOT=str(root/'pins'),FAULT_TRACE_OUT=str(root/'failure.json'))
def save():
 (base/'qualification-launch.json').write_text(json.dumps(record,indent=2)+'\n')
 (root/'command-results.json').write_text(json.dumps(records,indent=2)+'\n')
def call(args,log,cwd=repo):
 record['commands'].append({'command':args,'cwd':str(cwd),'log':log});save()
 with (root/log).open('w') as out:r=subprocess.run(args,cwd=cwd,env=env,stdout=out,stderr=subprocess.STDOUT)
 records.append({'command':args,'cwd':str(cwd),'log':log,'exit_code':r.returncode});save();r.check_returncode()
def check(mode):
 events=[json.loads(x) for x in (root/f'{mode}-events.jsonl').read_text().splitlines()]
 passes={e.get('Test') for e in events if e['Action']=='pass'}
 for name in ['TestSeededGraphSignalOperationActorsReplay','TestGraphSignalCallerRetryAfterContention']:assert name in passes,name
 output=''.join(e.get('Output','') for e in events)
 assert 'TIER1_SEEDS test=TestSeededGraphSignalOperationActorsReplay first=1 last=1000 completed=1000 requested=1000' in output
 assert not any(e['Action']=='fail' for e in events)
 assert len(list((root/'pins').glob('*.json')))==6
 assert snapshot()==before
 record[f'{mode}_completed_bodies']=1000;record[f'{mode}_actual_exit_code']=0;save()
save()
try:
 for mode in ['normal','race']:
  binary=root/f'{mode}.test'
  call(['go','test','-p=1',*(['-race'] if mode=='race' else []),'-c','-o',str(binary),'./sim'],f'{mode}-compile.log')
  build=subprocess.check_output(['go','version','-m',str(binary)],text=True)
  assert ('-race=true' in build)==(mode=='race')
  (root/f'{mode}-binary.json').write_text(json.dumps({'sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'build_info':build},indent=2)+'\n')
  call(['go','tool','test2json','-t','-p','js-wf/sim',str(binary),'-test.run=^(TestSeededGraphSignalOperationActorsReplay|TestGraphSignalCallerRetryAfterContention)$','-test.count=1','-test.timeout=90m','-test.v=test2json'],f'{mode}-events.jsonl',repo/'sim')
  check(mode)
  call([str(binary),'-test.run=^TestPinnedRegressionCorpus$/^graph-signal-actors-caller-retry[.]json$','-test.count=1','-test.timeout=5m','-test.v'],f'{mode}-pin.log',repo/'sim')
 record['terminal_acceptance']=True
finally:
 after=snapshot();(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n');assert after==before
 record['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('focused normal/race qualification accepted')
