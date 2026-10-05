from pathlib import Path
import json,subprocess,hashlib,time,os,datetime
r=Path('/tmp/js-wf-held-metadata-rollout-10m-20261005');out=Path('/tmp/js-wf-held-metadata-rollout-10m-live-20261005');out.mkdir()
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
ready_deadline=time.monotonic()+180
while True:
 try:
  b=json.loads((r/'binary.json').read_text());assert sha(r/'integration.test')==b['sha256'];break
 except (FileNotFoundError,json.JSONDecodeError):
  if time.monotonic()>=ready_deadline:raise
  time.sleep(.25)
seen=set();observed_parent=False;end=time.monotonic()+20*60
with (out/'sdks.jsonl').open('w') as log:
 while time.monotonic()<end:
  found_parent=False
  for p in Path('/proc').glob('[0-9]*'):
   try:
    args=(p/'cmdline').read_text().split('\0')[:-1]
    if not args or args[0]!=str(r/'integration.test'):continue
    parent='TestFiveContainerMixedWorkerKilledEveryFiveSeconds' in args[1]
    if parent:found_parent=True;observed_parent=True
    if int(p.name) in seen:continue
    digest=sha(p/'exe');assert digest==b['sha256']
    info=subprocess.check_output(['go','version','-m',str(p/'exe')],text=True,stderr=subprocess.DEVNULL);assert info.splitlines()[1:]==b['build_info'].splitlines()[1:]
    item={'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':int(p.name),'args':args,'sha256':digest,'build_info':info,'stat':(p/'stat').read_text()}
    log.write(json.dumps(item)+'\n');log.flush();os.fsync(log.fileno());seen.add(int(p.name))
   except (FileNotFoundError,ProcessLookupError,PermissionError,subprocess.CalledProcessError):continue
  if observed_parent and not found_parent:break
  time.sleep(.5)
(out/'watch-result.json').write_text(json.dumps({'observed_parent':observed_parent,'seen_pids':sorted(seen),'parent_gone':observed_parent and not found_parent,'observation_deadline_exhausted':time.monotonic()>=end},indent=2)+'\n')
(out/'executed-watch.py').write_bytes(Path(__file__).read_bytes())
print('WATCH_FINISHED',len(seen),flush=True)
