#!/usr/bin/env python3
"""Qualify current compiled CLI opt-ins from clean immutable Git source."""
import datetime,gzip,hashlib,json,os,subprocess,time
from pathlib import Path
here=Path(__file__).resolve().parent
source=json.loads((here/'source.json').read_text());checkout=Path(source['checkout'])
root=Path('/home/exedev/js-wf-compiled-canonical-cli-artifacts-20261010');root.mkdir()
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def inventory():
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source['source']
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
    names=subprocess.check_output(['git','ls-files'],cwd=checkout,text=True).splitlines()
    return {n:hashlib.sha256((checkout/n).read_bytes()).hexdigest() for n in names
            if n.split('/')[0] not in ('docs','scripts','.github') and (n.endswith('.go') or n in ('go.mod','go.sum') or '/testdata/' in n)}
before=inventory()
(here/'source-before.json.gz').write_bytes(gzip.compress(json.dumps(before,sort_keys=True).encode(),mtime=0))
configuration={'WF_OPERATOR_STANDALONE':'1','WF_OPERATOR_TEST_ROOT':str(root),'GOWORK':'off','GOFLAGS':''}
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')};env.update(configuration)
command=['/usr/local/bin/go','test','-race','./cmd/wf','-count=1','-v','-timeout=20m']
state=dict(**source,root=str(root),invocation=os.environ['INVOCATION_ID'],unit='js-wf-compiled-canonical-cli-20261010.service',
           supervisor_pid=os.getpid(),command=command,configuration=configuration,started_utc=stamp(),terminal=False,child_exit=None)
def save():
    tmp=here/'state.tmp';tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(here/'state.json')
save();start=time.monotonic()
with (here/'race.log').open('xb') as output:
    child=subprocess.Popen(command,cwd=checkout,env=env,stdout=output,stderr=subprocess.STDOUT)
    state['child_pid']=child.pid;save()
    code=child.wait()
state.update(child_exit=code,child_ended_utc=stamp(),child_wall_seconds=time.monotonic()-start);save()
after=inventory()
(here/'source-after.json.gz').write_bytes(gzip.compress(json.dumps(after,sort_keys=True).encode(),mtime=0))
state.update(terminal=True,inputs_unchanged=before==after,ended_utc=stamp(),log_sha256=hashlib.sha256((here/'race.log').read_bytes()).hexdigest())
assert before==after
save();print(json.dumps(state),flush=True)
raise SystemExit(code)
