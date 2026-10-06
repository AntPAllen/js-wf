from pathlib import Path
import subprocess,json,hashlib,time,os
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-outer-handler-baseline-control-20261006');root.mkdir()
source='ca7a1fa'
baseline=subprocess.check_output(['git','show',source+':worker/worker.go'],cwd=repo)
(root/'baseline-worker.go').write_bytes(baseline)
overlay={'Replace':{str(repo/'worker/worker.go'):str(root/'baseline-worker.go')}}
(root/'overlay.json').write_text(json.dumps(overlay))
command=['go','test','-race','-overlay='+str(root/'overlay.json'),'./sim','-run','^TestOuterHandlerCancellationHandoffAndLateAppend$','-count=1','-timeout=30s']
started=time.monotonic();r=subprocess.run(command,cwd=repo,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=90)
(root/'control.log').write_text(r.stdout)
assert r.returncode!=0 and 'outer handler trapped delivery: context deadline exceeded' in r.stdout,r.stdout
assert (repo/'worker/worker.go').read_bytes()==subprocess.check_output(['git','show','HEAD:worker/worker.go'],cwd=repo)
out=repo/'docs/scale/outer-handler-cancellation-2026-10-06';out.mkdir(exist_ok=True)
report=dict(command=command,baseline_source=source,baseline_worker_sha256=hashlib.sha256(baseline).hexdigest(),exit_code=r.returncode,elapsed_seconds=time.monotonic()-started,expected_rejection='outer handler trapped delivery: context deadline exceeded',expected_rejection_observed=True,scope='In-memory production-worker regression rejects pre-fix outer handler boundary via compiler overlay. No NATS startup or old store opening; no matrix/release qualification.')
(out/'baseline-control.json').write_text(json.dumps(report,indent=2)+'\n');(out/'baseline-control.log').write_text(r.stdout);(out/'executed-baseline-control.py').write_bytes(Path(__file__).read_bytes())
print('EXPECTED_BASELINE_REJECTION',r.returncode,flush=True)
