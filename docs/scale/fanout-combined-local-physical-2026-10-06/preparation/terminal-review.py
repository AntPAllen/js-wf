from pathlib import Path
import subprocess,json,time,shutil,traceback
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-fanout-combined-local-physical-20261006');out=repo/'docs/scale/fanout-combined-local-physical-2026-10-06/native'
unit='js-wf-fanout-combined-local-physical-20261006.service';end=time.monotonic()+40*60
while True:
 state=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','ExecMainStatus'],text=True)
 if 'ActiveState=inactive' in state or 'ActiveState=failed' in state:break
 assert time.monotonic()<end,'Observation timeout: inspect same native handle; no restart'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text()) if (root/'execution.json').exists() else None
if e:assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
qualified=False
try:
 assert 'ExecMainStatus=0' in state and e and e['status']=='passed' and e['exit_code']==0,'native producer did not pass'
 subprocess.run(['python3',str(repo/'scripts/review-fanout-combined.py'),'--root',str(root),'--output',str(root/'independent-review.json')],cwd=repo,check=True)
 qualified=True
except BaseException:
 assert root.exists(),'producer failed before fixture existed'
 (root/'failure-review.json').write_text(json.dumps({'unit_state':state,'execution':e,'native_qualified':False,'review_error':traceback.format_exc(),'scope':'Original unsuccessful native/review preserved; no physical qualification claim'},indent=2)+'\n')
shutil.copy2(__file__,root/'executed-terminal-review.py')
for name in ('review-fanout-combined.py','review-tier2-copied-audit.py','check-fanout-boundary-matrix.py'):
 shutil.copy2(repo/'scripts'/name,root/('executed-'+name))
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
for name in ('independent-review.json','failure-review.json','execution.json'):
 if (root/name).exists():shutil.copy2(root/name,out/name)
print('FANOUT_LOCAL_PHYSICAL_NATIVE_REVIEW_AND_ARCHIVE_COMPLETE',qualified,flush=True)
if not qualified:raise SystemExit(1)
