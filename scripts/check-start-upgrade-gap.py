#!/usr/bin/env python3
"""Qualify a real Start-process crash carried across a retained-store upgrade."""
import argparse,hashlib,json,os,subprocess
from pathlib import Path
REPO=Path(__file__).resolve().parents[1]
TEST='TestMixedVersionRollingUpgradeFallback'
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);a=p.parse_args();root=a.root.resolve()
assert not root.exists() and not root.is_relative_to(REPO)
assert not subprocess.check_output(['git','status','--porcelain'],cwd=REPO)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
names=subprocess.check_output(['git','ls-files'],cwd=REPO,text=True).splitlines()
def inventory():return {n:hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum')}
root.mkdir();before=inventory();(root/'source-before.json').write_text(json.dumps(dict(revision=revision,files=before),indent=2)+'\n')
source=REPO/'reconcile/starts.go';text=source.read_text();needle='return p.client.Enqueue(ctx, typ, id, "start:"+typ+"."+id+":"+strconv.FormatUint(sequence, 10))'
assert text.count(needle)==1
control=root/'skip-gap-repair.go.txt';control.write_text(text.replace(needle,'if typ == "mixed-upgrade" && strings.HasPrefix(id, "gap-upgrade-") { return nil }\n\t'+needle))
overlay=root/'overlay.json';overlay.write_text(json.dumps({'Replace':{str(source):str(control)}})+'\n')
commands=[];cases=[]
try:
 for mode in ('positive','skip-repair'):
  case=root/mode;case.mkdir()
  env=dict(os.environ,GOMEMLIMIT='512MiB',GOMAXPROCS='2',WF_MIXED_UPGRADE_ARTIFACT_ROOT=str(case/'artifacts'))
  binary=case/'integration.test';cmd=['go','test','-p=1','-race','-c','-o',str(binary)]
  if mode=='skip-repair':cmd+=['-overlay='+str(overlay)]
  cmd+=['./integration'];commands.append(dict(command=cmd,cwd=str(REPO)))
  (root/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');subprocess.run(cmd,cwd=REPO,env=env,check=True)
  info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert '-race=true' in info
  (case/'binary.json').write_text(json.dumps(dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),build_info=info),indent=2)+'\n')
  pattern='^'+TEST+'$' if mode=='positive' else '^'+TEST+'/old-peer-first$'
  cmd=['go','tool','test2json','-t','-p','js-wf/integration',str(binary),'-test.v=test2json','-test.run='+pattern,'-test.count=1','-test.timeout=4m']
  commands.append(dict(command=cmd,cwd=str(REPO),environment={k:env[k] for k in ('GOMEMLIMIT','GOMAXPROCS','WF_NATS_SERVER_BIN','WF_MIXED_UPGRADE_ARTIFACT_ROOT')}))
  (root/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
  with (case/'events.jsonl').open('w') as out,(case/'stderr.log').open('w') as err:result=subprocess.run(cmd,cwd=REPO,env=env,stdout=out,stderr=err)
  events=[json.loads(l) for l in (case/'events.jsonl').read_text().splitlines()];want='pass' if mode=='positive' else 'fail'
  assert not any(e['Action'] in ('skip','build-fail') for e in events)
  assert [e['Action'] for e in events if e.get('Test')==TEST and e['Action'] in ('pass','fail')]==[want]
  assert [e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass','fail')]==[want]
  output=''.join(e.get('Output','') for e in events);assert 'panic:' not in output and 'timed out' not in output
  profiles=['old-peer-first','auto-fallback-on-new-peer'] if mode=='positive' else ['old-peer-first']
  for profile in profiles:
   proof=json.loads((case/'artifacts'/profile/'start-gap/killed-gap.json').read_text())
   assert proof['signal']=='SIGKILL' and proof['receipt']['version']=='2.11.17' and proof['run_messages']==0 and proof['journal_absent']
   assert proof['receipt']['invocation']['Sequence']==proof['retained']['Sequence']>0
  if mode=='positive':
   assert result.returncode==0
   for profile in profiles:assert [e['Action'] for e in events if e.get('Test')==TEST+'/'+profile and e['Action'] in ('pass','fail')]==['pass']
   assert output.count('START_UPGRADE_GAP_REPAIRED ')==2
  else:
   assert result.returncode!=0 and output.count('post-upgrade process-gap repair produced no journal')==1
  cases.append(dict(mode=mode,verdict=want,package_seconds=next(e['Elapsed'] for e in events if not e.get('Test') and e['Action']==want)))
 (root/'result.json').write_text(json.dumps(dict(accepted=True,source=revision,cases=cases,scope='Real R3 mixed-version Start process crash, retained upgrade and scanner repair; not the full R5 mixed row/200 seeds/24h.'),indent=2)+'\n')
finally:
 after=inventory();(root/'source-after.json').write_text(json.dumps(dict(revision=revision,files=after),indent=2)+'\n');assert before==after
