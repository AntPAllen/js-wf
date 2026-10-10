"""Qualify provenance fix against closed real native bundles and old-reader control."""
import datetime, hashlib, json, os, subprocess, time
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
checkout=Path('/home/exedev/js-wf-offline-child-provenance-qualification')
root=Path('/home/exedev/js-wf-offline-child-provenance-20261010')
native_base=repo/'docs/scale/graph-continuation-child-offline-2026-10-10'
native_root=Path('/home/exedev/js-wf-child-offline-20261010')
source=subprocess.check_output(['git','rev-parse','57393ac'],cwd=checkout,text=True).strip()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
assert not root.exists()
root.mkdir(); (root/'artifacts').mkdir()
state=dict(source=source,checkout=str(checkout),root=str(root),pid=os.getpid(),invocation=os.environ.get('INVOCATION_ID'),started=None,commands=[],replays=[],accepted=False)
def stamp(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save(): (base/'state.json').write_text(json.dumps(state,indent=2)+'\n')
state['started']=stamp(); state['phase']='waiting_for_native_terminal'; save()
# Poll the actual producer unit; never restart or infer completion from elapsed time.
deadline=time.monotonic()+1200
while True:
    properties=subprocess.check_output(['systemctl','--user','show','js-wf-child-offline-20261010.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True)
    if 'MainPID=0\n' in properties:
        native=json.loads((native_base/'state.json').read_text())
        assert native['exit']==0 and 'ExecMainStatus=0\n' in properties
        (root/'native-supervisor-exit.txt').write_text(properties)
        break
    assert time.monotonic()<deadline
    time.sleep(10)
state['phase']='native_review';save()
names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()
names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
def inventory(): return {n:hashlib.sha256((checkout/n).read_bytes()).hexdigest() for n in names}
(root/'source-before.json').write_text(json.dumps(dict(source=source,files=inventory()),indent=2)+'\n')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_OPERATOR_TEST_ROOT=str(root/'artifacts'))
def run(args,filename,cwd=checkout):
    row=dict(command=args,cwd=str(cwd),started=stamp());state['commands'].append(row);save()
    with (root/filename).open('wb') as output: row['exit']=subprocess.call(args,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT)
    row['finished']=stamp();save();assert row['exit']==0
run(['python3',str(native_base/'review.py')],'native-review.log',repo)
assert json.loads((native_base/'review.json').read_text())['accepted']
state['native_source']=native['source'];state['native_review_sha256']=hashlib.sha256((native_base/'review.py').read_bytes()).hexdigest()
state['phase']='compile';save()
cli,plugin,binary=root/'wf',root/'handler.so',root/'cli-race.test'
run(['go','test','-race','-c','-o',str(binary),'./cmd/wf'],'compile-tests.log')
run(['go','tool','test2json','-t','-p','js-wf/cmd/wf',str(binary),'-test.v=test2json','-test.run=^(TestReplayGraphChildProvenance|TestReplayContinuationPlugin)$','-test.count=1','-test.timeout=10m'],'events.jsonl',checkout/'cmd/wf')
run(['go','build','-race','-o',str(cli),'./cmd/wf'],'compile-cli.log')
run(['go','build','-race','-buildmode=plugin','-o',str(plugin),'./worker/testdata/graphreplayplugin'],'compile-plugin.log')
state['phase']='offline_replay';save()
marker=root/'effect'
def replay(path,cli_path,symbol,plugin_path,expected,label):
    args=[str(cli_path),'-url','nats://127.0.0.1:1','-handler-plugin',str(plugin_path),'-handler-symbol',symbol,'-replay-bundle',str(path),'replay']
    row=dict(label=label,input=str(path),input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),command=args,started=stamp(),expected=expected)
    result=subprocess.run(args,env=dict(env,WF_REPLAY_EFFECT_MARKER=str(marker)),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=15)
    row.update(exit=result.returncode,output=result.stdout.decode(),finished=stamp());state['replays'].append(row);save()
    if expected in ('completed','suspended'):
        report=json.loads(result.stdout); assert result.returncode==0 and report['status']==expected
        if expected=='completed': assert report['result']==60
    else:
        assert result.returncode!=0 and expected in row['output']
    assert not marker.exists()
    return row
exports=sorted(p for p in (native_root/'artifacts').rglob('*.json') if p.name in ('completed.json','suspended.json','child-pending.json','missing-objects.json','missing-child-result.json'))
assert len(exports)==76
for path in exports:
    bundle=json.loads(path.read_text());child=isinstance(json.loads(__import__('base64').b64decode(bundle['input'])),dict)
    symbol='ChildWorkflow' if child else 'Workflow'
    expected='offline replay object is missing' if path.name.startswith('missing-') else 'completed' if path.name=='completed.json' else 'suspended'
    replay(path,cli,symbol,plugin,expected,'native-'+path.name)
    if path.name=='completed.json': replay(path,cli,'MissingChildContinuation' if child else 'MissingContinuation',plugin,'continuation stage is not registered','native-missing-stage')
diag=Path('/home/exedev/js-wf-child-offline-audit-diagnostic-20261010')
old=json.loads((diag/'report.json').read_text())
for row in old['rows']:
    path=diag/(row['name']+'.json')
    assert hashlib.sha256(path.read_bytes()).hexdigest()==row['input_sha256']
    replay(path,cli,'ChildWorkflow',plugin,'completed' if row['name']=='control' else 'invalid step protocol in journal','diagnostic-fixed-'+row['name'])
    # The original old-reader defect is a required negative control, preserved separately.
    replay(path,Path(old['cli']),'ChildWorkflow',Path(old['plugin']),'completed','diagnostic-old-'+row['name'])
(root/'source-after.json').write_text(json.dumps(dict(source=source,files=inventory()),indent=2)+'\n')
state['binaries']={p.name:dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in (binary,cli,plugin)}
state.update(phase='closed',finished=stamp(),exit=0);save()
