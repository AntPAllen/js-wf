import hashlib,json,os,pathlib,subprocess,sys,time
root=pathlib.Path.cwd()
out=root/'docs/scale/graph-signal-repair-2026-10-08/qualification'
mode=sys.argv[1]
source=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
paths=[p for p in subprocess.check_output(['git','ls-files'],text=True).splitlines() if p.endswith(('.go','.yml','.yaml')) or p in ('go.mod','go.sum') or p.startswith('sim/testdata/')]
def capture():
 result={}
 for p in paths:
  content=(root/p).read_bytes()
  committed=subprocess.check_output(['git','show',source+':'+p])
  assert content==committed,p
  result[p]=hashlib.sha256(content).hexdigest()
 return result
before=capture()
(out/(mode+'-source-before.json')).write_text(json.dumps({'source':source,'inputs':before},indent=2)+'\n')
commands=[('components',['./journal','./reconcile','./worker','-run','^TestGraphCanonicalSignal|^TestNativeCanonicalSignalQueue|^TestNativeGraphReconcileCanonicalSignals$']),('client',['./client']),('pins',['./sim','-run','^TestPinnedRegressionCorpus$'])]
# Require the actual pinned-corpus selector rather than accepting a no-tests pass.
for p in (root/'sim').glob('*test.go'):
 text=p.read_text()
 if 'func TestPinnedRegressionCorpus(' in text: break
else:
 raise RuntimeError('Pinned corpus selector not found')
verdicts=[]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
for name,args in commands:
 cmd=['go','test','-p=1']+(['-race'] if mode=='race' else [])+args+['-json','-count=1','-timeout=5m']
 start=time.time()
 with (out/(mode+'-'+name+'.jsonl')).open('w') as log:
  p=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
 verdicts.append({'name':name,'command':cmd,'exit_code':p.returncode,'wall_seconds':time.time()-start})
 (out/(mode+'-commands.json')).write_text(json.dumps({'source':source,'commands':verdicts},indent=2)+'\n')
 if p.returncode: break
assert capture()==before
(out/(mode+'-source-after.json')).write_text(json.dumps({'source':source,'inputs':before},indent=2)+'\n')
sys.exit(0 if len(verdicts)==len(commands) and all(v['exit_code']==0 for v in verdicts) else 1)
