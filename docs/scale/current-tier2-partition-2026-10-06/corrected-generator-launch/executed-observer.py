from pathlib import Path
import subprocess,json,time,datetime,sys,hashlib,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
root=Path('/tmp/js-wf-tier2-partition200-watch-37500390198-20261006')
assert not root.exists();root.mkdir()
run_id=37500390198;source='d9093d74f4e18562d70173c0fc6715c394a0314a'
(root/'executed-watch.py').write_bytes(Path(__file__).read_bytes())
(root/'fixture-archive-executed.py').write_bytes((repo/'scripts/fixture_archive.py').read_bytes())
(root/'fixture-delta-executed.py').write_bytes((repo/'scripts/fixture_delta.py').read_bytes())
started=time.monotonic()
def save(name,value):(root/name).write_text(json.dumps(value,indent=2)+'\n')
save('observer.json',dict(pid=os.getpid(),run_id=run_id,source=source,poll_seconds=300,maximum_observation_days=35,requested_row='partition',requested_seeds=200,requested_duration='10m',scope='Read-only observation/collection; never cancels or restarts native jobs and never qualifies runtime. Observation expiry is not native terminal.'))
while time.monotonic()-started<35*86400:
 try:
  data=subprocess.check_output(['gh','run','view',str(run_id),'--json','status,conclusion,headSha,jobs'],cwd=repo,text=True,timeout=120)
  current=json.loads(data);assert current['headSha']==source
  save('latest.json',current)
  counts={}
  for job in current['jobs']:
   key=job['status']+':'+job['conclusion'];counts[key]=counts.get(key,0)+1
  with (root/'observations.jsonl').open('a') as stream:stream.write(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),status=current['status'],conclusion=current['conclusion'],jobs=len(current['jobs']),counts=counts))+'\n')
  if current['status']=='completed':break
 except (subprocess.SubprocessError,ValueError) as exc:
  with (root/'observation-errors.jsonl').open('a') as stream:stream.write(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),error=str(exc)))+'\n')
 time.sleep(300)
else:
 save('observation-expired.json',dict(native_terminal=False,scope='Observer expired; retain original run handle and re-poll. No restart authorized by observation expiry.'));raise SystemExit(0)
assert current['status']=='completed'
save('terminal.json',current)
with (root/'logs.zip').open('wb') as stream:
 subprocess.run(['gh','api','--allow-escape-sequences',f'repos/AntPAllen/js-wf/actions/runs/{run_id}/logs'],cwd=repo,stdout=stream,check=True,timeout=900)
for pattern,destination in [('matrix-partition-*-10m','artifacts'),('workload-source-partition-*','source-artifacts')]:
 command=['gh','run','download',str(run_id),'--pattern',pattern,'--dir',str(root/destination)]
 result=subprocess.run(command,cwd=repo,capture_output=True,text=True,timeout=1800)
 (root/(destination+'-download.stdout')).write_text(result.stdout)
 (root/(destination+'-download.stderr')).write_text(result.stderr)
 save(destination+'-download.json',dict(command=command,exit_code=result.returncode))
save('collection.json',dict(source=source,run_id=run_id,native_status=current['status'],native_conclusion=current['conclusion'],qualified=False,scope='Full available terminal provider logs/artifacts collected. Missing/failed artifact downloads preserved as diagnostics; no release gate qualified. Independent source/job/coverage/raw history/fault/latency/duration review remains required.'))
proof=fixture_archive.capture(root,Path(str(root)+'-complete.tar.gz'),Path(str(root)+'-proof'),compresslevel=1)
print('TERMINAL_COLLECTION_ONLY',json.dumps(proof),flush=True)
