#!/usr/bin/env python3
"""One full CLI race run, immutable logs and durable child exit receipt."""
import datetime, gzip, hashlib, json, os, subprocess, time
from pathlib import Path
here = Path(__file__).resolve().parent
repo = here.parents[2]
root = Path('/home/exedev/js-wf-cli-full-restored-20261010')
root.mkdir(exist_ok=False)
command = ['/usr/local/bin/go', 'test', '-race', './cmd/wf', '-timeout=10m', '-count=1', '-v']
def inputs():
    names = subprocess.check_output(['git','ls-files'], cwd=repo, text=True).splitlines()
    return {n: hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in names
            if n.endswith('.go') or n in ('go.mod','go.sum') or '/testdata/' in n}
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save():
    tmp = here/'restored-full-state.tmp'
    tmp.write_text(json.dumps(state, indent=2)+'\n')
    tmp.replace(here/'restored-full-state.json')
before=inputs()
(here/'restored-full-inputs-before.json.gz').write_bytes(gzip.compress(json.dumps(before,sort_keys=True).encode(),mtime=0))
(here/'restored-graph_test.go.txt.gz').write_bytes(gzip.compress((repo/'cmd/wf/graph_test.go').read_bytes(),mtime=0))
for name in ('visibility/graph.go','visibility/graph_test.go'):
    (here/('restored-'+name.replace('/','_')+'.txt.gz')).write_bytes(gzip.compress((repo/name).read_bytes(),mtime=0))
state=dict(source_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
           command=command, retained_root=str(root), started_utc=now(), exit=None,
           environment={'WF_OPERATOR_TEST_ROOT':str(root),'GOWORK':'off','GOFLAGS':''})
env=os.environ.copy(); env.update(state['environment'])
started=time.monotonic()
with (here/'restored-full-race.log').open('xb') as log:
    child=subprocess.Popen(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
    state['pid']=child.pid; save()
    state['exit']=child.wait()
state.update(ended_utc=now(),wall_seconds=time.monotonic()-started)
after=inputs()
(here/'restored-full-inputs-after.json.gz').write_bytes(gzip.compress(json.dumps(after,sort_keys=True).encode(),mtime=0))
state['inputs_unchanged']=before==after
state['log_sha256']=hashlib.sha256((here/'restored-full-race.log').read_bytes()).hexdigest()
save()
print(json.dumps(state),flush=True)
raise SystemExit(state['exit'])
