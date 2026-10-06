from pathlib import Path
import subprocess,json,time,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier2-retained-isolation-ten-minute-20261006');out=repo/'docs/scale/tier2-retained-isolation-2026-10-06/native-ten-minute'
unit='js-wf-tier2-retained-isolation-ten-minute-20261006.service'
end=time.monotonic()+23*60
while True:
 state=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','ExecMainStatus'],text=True)
 if 'ActiveState=inactive' in state or 'ActiveState=failed' in state:break
 assert time.monotonic()<end,'Observation timeout: inspect same live unit; no automatic restart'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed','interrupted') and not Path('/proc/'+str(e['pid'])).exists()
for file in ('observed-servers.json','observed-workers.json'):
 for record in json.loads((root/file).read_text()):assert not Path('/proc/'+str(record['pid'])).exists()
qualified='ExecMainStatus=0' in state and e['status']=='passed' and e['exit_code']==0
if qualified:
 subprocess.run(['python3','/tmp/js-wf-review-tier2-retained-isolation-ten-minute-20261006.py'],cwd=repo,check=True)
else:
 (root/'terminal-failure-review.json').write_text(json.dumps({'unit_state':state,'execution':e,'observed_processes_closed':True,'native_qualified':False,'scope':'Closed failed campaign preserved; no fault/duration/store qualification'},indent=2)+'\n')
shutil.copy2(__file__,root/'executed-terminal-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
for name in ('execution.json','review.json','terminal-failure-review.json'):
 if (root/name).exists():shutil.copy2(root/name,out/name)
print('ISOLATION_TERMINAL_REVIEW_AND_ARCHIVE_COMPLETE',qualified,flush=True)
