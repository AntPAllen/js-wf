"""Require terminal retained supervisor and exact eight native cancellation cases."""
import hashlib,io,json,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent;s=json.loads((base/'state.json').read_text());root=Path(s['root']);checkout=Path(s['checkout'])
assert s.get('phase')=='closed' and s['exit']==0
p=subprocess.check_output(['systemctl','--user','show','js-wf-running-effect-cancel-20261010.service','-p','LoadState','-p','MainPID','-p','ExecMainStatus','-p','InvocationID'],text=True)
assert 'LoadState=loaded\n' in p and 'MainPID=0\n' in p and 'ExecMainStatus=0\n' in p and 'InvocationID='+s['invocation']+'\n' in p
(root/'supervisor-exit.txt').write_text(p)
def hash(p):return hashlib.sha256(p.read_bytes()).hexdigest()
before,after=[json.loads((root/n).read_text()) for n in ('source-before.json','source-after.json')];assert before==after and before['source']==s['source']
names=subprocess.check_output(['git','ls-tree','-r','--name-only',s['source']],cwd=checkout,text=True).splitlines();names=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(names)==set(before['files'])
raw=subprocess.check_output(['git','cat-file','--batch'],cwd=checkout,input=''.join(s['source']+':'+n+'\n' for n in names).encode());stream=io.BytesIO(raw)
for n in names:
 h=stream.readline().decode().split();assert h[1]=='blob';body=stream.read(int(h[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][n]==hash(checkout/n)
assert len(s['commands'])==2 and all(r['exit']==0 and r['finished'] for r in s['commands'])
assert '-test.run=^TestNativeGraphContinuationCancelRunningEffect$' in s['commands'][1]['command']
binary=root/'worker-race.test';assert hash(binary)==s['binary']['sha256'] and binary.stat().st_size==s['binary']['bytes'];assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
rows=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()];assert not any(r['Action'] in ('fail','skip') or 'WARNING: DATA RACE' in r.get('Output','') for r in rows)
assert sum(r['Action']=='pass' and 'Test' not in r for r in rows)==1
expected={'TestNativeGraphContinuationCancelRunningEffect/'+d+'/archive='+a+'/late-success='+l for d in ('R1','R3Domain') for a in ('false','true') for l in ('false','true')}
assert {r['Test'] for r in rows if r['Action']=='pass' and '/late-success=' in r.get('Test','')}==expected
for name in expected:
 output=''.join(r.get('Output','') for r in rows if r.get('Test')==name)
 assert output.count('RUNNING_EFFECT_CANCELLED ')==1
 assert 'terminals=1 cancellations=1 requests=1' in output and 'effects=3 initial=1 next=1 finish=1 immutable_duplicate=true' in output
 assert 'notification_disabled=true source_removed=true retried=true' in output
result=dict(accepted=True,source=s['source'],git_verified_inputs=len(names),actual_supervisor_exit=0,supervisor_exit_proven=True,native_race_cases=8,events_sha256=hash(root/'events.jsonl'),scope='Restored running RunOnce cancellation, cooperative error and late success; R1/R3-domain and live/archive checkpoints. Full faults, uncooperative effects, retention, import, admission and original broader gates remain open.')
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
