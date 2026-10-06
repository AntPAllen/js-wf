from pathlib import Path
import sys,subprocess,json,time,datetime,hashlib,importlib.util,shutil,traceback
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-postgres-domain-projection-corrected-50000-20261006');out=Path('/tmp/js-wf-pg-domain-terminal-review-20261006');out.mkdir()
unit='js-wf-postgres-domain-projection-corrected-50000-20261006.service'
pin=out/'pinned-reviewer.py';shutil.copyfile(repo/'scripts/review-postgres-domain-projection.py',pin)
pin_sha=hashlib.sha256(pin.read_bytes()).hexdigest();shutil.copyfile(__file__,out/'executed-waiter.py')
started=time.monotonic()
while True:
 values=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
 observation=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),unit=values,reviewer_sha256=pin_sha,original_handle=unit)
 (out/'latest.json').write_text(json.dumps(observation,indent=2)+'\n')
 if values['ActiveState'] in ('inactive','failed') and values['MainPID']=='0':break
 if time.monotonic()-started>75*60:raise RuntimeError('Observer expired; original native handle must be re-polled, never restarted due to observer expiry')
 time.sleep(30)
if values['ActiveState']!='inactive' or values['ExecMainStatus']!='0':
 observation.update(qualification=None,rejection='Original producer is terminal failure; no success qualification. Existing originals retained.')
else:
 try:
  assert hashlib.sha256(pin.read_bytes()).hexdigest()==pin_sha
  sys.path.insert(0,str(repo/'scripts'))
  spec=importlib.util.spec_from_file_location('pinned',pin);review=importlib.util.module_from_spec(spec);spec.loader.exec_module(review)
  review.REPO=repo
  destination=root.with_name(root.name+'-proof')/'independent-review.json'
  sys.argv=[str(pin),'--root',str(root),'--output',str(destination)]
  review.main()
  observation.update(qualification=str(destination),scope='Pinned terminal reviewer with explicit repository root; no native restart, Git commit, S3 upload or local deletion.')
 except BaseException:
  observation.update(qualification=None,rejection=traceback.format_exc())
(out/'terminal-review.json').write_text(json.dumps(observation,indent=2)+'\n')
print(json.dumps({k:observation[k] for k in observation if k in ('qualification','rejection')}),flush=True)
