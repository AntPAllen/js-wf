from pathlib import Path
import subprocess,json,time,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier2-fanout-copied-audit-20261006');out=repo/'docs/scale/tier2-retained-fanout-2026-10-06/copied-audit'
unit='js-wf-tier2-fanout-copied-audit-20261006.service'
end=time.monotonic()+15*60
while True:
 state=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','ExecMainStatus'],text=True)
 if 'ActiveState=inactive' in state or 'ActiveState=failed' in state:break
 assert time.monotonic()<end,'Observation timeout: inspect same copied native handle; no restart'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text()) if (root/'execution.json').exists() else None
if e:assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
if (root/'observed-servers.json').exists():
 for server in json.loads((root/'observed-servers.json').read_text()):assert not Path('/proc/'+str(server['pid'])).exists()
qualified='ExecMainStatus=0' in state and e and e['status']=='passed' and e['exit_code']==0
if qualified:
 subprocess.run(['python3','/tmp/js-wf-review-pause-ten-minute-copied-20261006.py',str(root),'v2'],cwd=repo,check=True)
else:
 (root/'failure-review.json').write_text(json.dumps({'unit_state':state,'execution':e,'copied_audit_qualified':False,'scope':'Closed unsuccessful copied audit preserved; no data absence or original native gate reclassification'},indent=2)+'\n')
shutil.copy2(__file__,root/'executed-terminal-review.py');shutil.copy2('/tmp/js-wf-review-pause-ten-minute-copied-20261006.py',root/'executed-independent-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
for name in ('independent-review.json','failure-review.json','execution.json','precopy-verification.json'):
 if (root/name).exists():shutil.copy2(root/name,out/name)
print('COPIED_FANOUT_REVIEW_AND_ARCHIVE_COMPLETE',bool(qualified),flush=True)
