from pathlib import Path
import subprocess,json,time,shutil,traceback
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier2-retained-upgrade-ten-minute-20261006');out=repo/'docs/scale/tier2-retained-upgrade-2026-10-06/native-ten-minute'
unit='js-wf-tier2-retained-upgrade-ten-minute-20261006.service'
end=time.monotonic()+23*60
while True:
 state=subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','SubState','-p','ExecMainStatus'],text=True)
 if 'ActiveState=inactive' in state or 'ActiveState=failed' in state:break
 assert time.monotonic()<end,'Observation timeout: inspect same native handle; no restart'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text()) if (root/'execution.json').exists() else None
if e:assert e['status'] in ('passed','failed','interrupted') and not Path('/proc/'+str(e['pid'])).exists()
for name in ('observed-servers.json','observed-workers.json'):
 if (root/name).exists():
  for record in json.loads((root/name).read_text()):assert not Path('/proc/'+str(record['pid'])).exists()
qualified=False
try:
 assert 'ExecMainStatus=0' in state and e and e['status']=='passed' and e['exit_code']==0,'original producer did not pass'
 subprocess.run(['python3','/tmp/js-wf-review-upgrade-ten-minute-20261006.py','--root',str(root),'--output',str(root/'review.json')],cwd=repo,check=True)
 review=json.loads((root/'review.json').read_text())
 (root/'expected-original-report.json').write_text(json.dumps(review['expected_original_report'],indent=2)+'\n')
 qualified=True
except BaseException:
 (root/'terminal-failure-review.json').write_text(json.dumps({'unit_state':state,'execution':e,'native_qualified':False,'review_error':traceback.format_exc(),'scope':'Original terminal result and complete failed-review evidence preserved; no qualification claim'},indent=2)+'\n')
shutil.copy2(__file__,root/'executed-terminal-review.py');shutil.copy2(Path('/tmp/js-wf-review-upgrade-ten-minute-20261006.py'),root/'executed-native-review.py');shutil.copy2(repo/'scripts/review-tier2-retained-workers.py',root/'executed-common-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
for name in ('execution.json','review.json','terminal-failure-review.json'):
 if (root/name).exists():shutil.copy2(root/name,out/name)
print('UPGRADE_TERMINAL_REVIEW_AND_ARCHIVE_COMPLETE',qualified,flush=True)
if not qualified:raise SystemExit(1)
