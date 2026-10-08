import hashlib,json,os,pathlib,subprocess,sys,time
root=pathlib.Path.cwd()
out=pathlib.Path(sys.argv[2]).resolve()
mode=sys.argv[1]
source=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert source=='dd98e39c29c5e8689f06fdbdcca7e944b53c0963'
paths=[p for p in subprocess.check_output(['git','ls-files'],text=True).splitlines() if p.endswith(('.go','.yml','.yaml')) or p in ('go.mod','go.sum') or p.startswith('sim/testdata/')]
def capture():
 result={}
 for p in paths:
  body=(root/p).read_bytes()
  assert body==subprocess.check_output(['git','show',source+':'+p]),p
  result[p]=hashlib.sha256(body).hexdigest()
 return result
before=capture()
(out/(mode+'-source-before.json')).write_text(json.dumps({'source':source,'inputs':before},indent=2)+'\n')
commands=[('family','^TestSeededGraphSignalRuntimeReplay$'),('pins','^TestPinnedRegressionCorpus$')]
verdicts=[]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',FAULT_TRACE_OUT=str(out/(mode+'-failure-trace.json')))
for name,pattern in commands:
 cmd=['go','test','-p=1']+(['-race'] if mode=='race' else [])+['./sim','-run',pattern,'-json','-count=1','-timeout=20m']
 start=time.time()
 with (out/(mode+'-'+name+'.jsonl')).open('w') as log:
  p=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
 verdicts.append({'name':name,'command':cmd,'exit_code':p.returncode,'wall_seconds':time.time()-start})
 (out/(mode+'-commands.json')).write_text(json.dumps({'source':source,'cwd':str(root),'environment':{k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','SIM_SEEDS','SIM_COVERAGE_SUMMARY')},'commands':verdicts},indent=2)+'\n')
 if p.returncode:break
assert capture()==before
(out/(mode+'-source-after.json')).write_text(json.dumps({'source':source,'inputs':before},indent=2)+'\n')
sys.exit(0 if len(verdicts)==len(commands) and all(v['exit_code']==0 for v in verdicts) else 1)
