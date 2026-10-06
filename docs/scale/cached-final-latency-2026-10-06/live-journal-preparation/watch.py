from pathlib import Path
import subprocess,json,hashlib,time,datetime,os,shutil
root=Path('/tmp/js-wf-cached-latency-journal-10m-20261006');out=Path('/tmp/js-wf-cached-latency-journal-10m-watch-20261006');out.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def save(n,x):(out/n).write_text(json.dumps(x,indent=2)+'\n')
ready=time.monotonic()+180
while not (root/'binary.json').exists():
 assert time.monotonic()<ready,'Observation timeout: inspect same producer handle'
 time.sleep(.25)
binary=json.loads((root/'binary.json').read_text());assert sha(root/'integration.test')==binary['sha256']
parent=None;sdk_seen=set();server_seen=set();end=time.monotonic()+22*60
with (out/'sdks.jsonl').open('w') as sdks,(out/'servers.jsonl').open('w') as servers:
 while time.monotonic()<end:
  for proc in Path('/proc').glob('[0-9]*'):
   try:
    args=proc.joinpath('cmdline').read_text().split('\0')[:-1]
    if len(args)<2 or args[0]!=str(root/'integration.test'):continue
    pid=int(proc.name)
    if 'TestFiveContainerMixedJournalLeaderEveryThirtySeconds' in args[1]:parent=pid
    if pid in sdk_seen:continue
    stat=proc.joinpath('stat').read_text();start=stat.rsplit(')',1)[1].split()[19]
    digest=sha(proc/'exe');assert digest==binary['sha256']
    info=subprocess.check_output(['go','version','-m',str(proc/'exe')],text=True);assert info.splitlines()[1:]==binary['build_info'].splitlines()[1:]
    selected={}
    for entry in proc.joinpath('environ').read_bytes().split(b'\0'):
     key,sep,value=entry.partition(b'=')
     if sep and key.decode() in ('GOMAXPROCS','GOGC','GOMEMLIMIT','WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT','WF_MATRIX_CACHED_LATENCY_METADATA'):selected[key.decode()]=value.decode()
    assert proc.joinpath('stat').read_text().rsplit(')',1)[1].split()[19]==start
    item={'pid':pid,'args':args,'sha256':digest,'build_info':info,'stat':stat,'profile_environment':selected,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    sdks.write(json.dumps(item)+'\n');sdks.flush();os.fsync(sdks.fileno());sdk_seen.add(pid)
   except (FileNotFoundError,ProcessLookupError,PermissionError,subprocess.CalledProcessError):continue
  if parent:
   save('parent.json',{'pid':parent})
   ids=subprocess.check_output(['docker','ps','-q','--filter',f'name=js-wf-route-{parent}-'],text=True).split()
   for cid in ids:
    try:
     inspected=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0];pid=inspected['State']['Pid']
     if not pid or (cid,pid) in server_seen:continue
     proc=Path('/proc')/str(pid);stat=proc.joinpath('stat').read_text();start=stat.rsplit(')',1)[1].split()[19]
     digest=sha(proc/'exe');destination=out/'server-executables'/digest;destination.parent.mkdir(exist_ok=True)
     if not destination.exists():shutil.copyfile(proc/'exe',destination)
     assert sha(destination)==digest
     info=subprocess.check_output(['go','version','-m',str(proc/'exe')],text=True);assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in info
     assert proc.joinpath('stat').read_text().rsplit(')',1)[1].split()[19]==start
     item={'pid':pid,'container':cid,'sha256':digest,'build_info':info,'args':proc.joinpath('cmdline').read_text().split('\0')[:-1],'stat':stat,'inspect':inspected,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
     servers.write(json.dumps(item)+'\n');servers.flush();os.fsync(servers.fileno());server_seen.add((cid,pid))
    except (FileNotFoundError,ProcessLookupError,subprocess.CalledProcessError):continue
   if not Path('/proc/'+str(parent)).exists():break
  time.sleep(.5)
save('watch-result.json',{'parent_pid':parent,'parent_gone':bool(parent) and not Path('/proc/'+str(parent)).exists(),'sdk_pids':sorted(sdk_seen),'server_observations':len(server_seen),'deadline_exhausted':time.monotonic()>=end,'scope':'Periodic500ms actual process observations; no live stores read, not exhaustive short-lived process coverage'})
(out/'executed-watch.py').write_bytes(Path(__file__).read_bytes())
print('WATCH_FINISHED',parent,len(sdk_seen),len(server_seen),flush=True)
