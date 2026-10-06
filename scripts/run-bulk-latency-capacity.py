#!/usr/bin/env python3
from pathlib import Path
import subprocess,json,hashlib,os,shutil,time,datetime,argparse
parser=argparse.ArgumentParser(description='Fresh normal R5 full400k/4.8M bulk latency and process-memory capacity producer; never reopens a fixture.')
parser.add_argument('--root',required=True,type=Path)
args=parser.parse_args()
repo=Path(__file__).resolve().parent.parent;root=args.root
assert root.is_absolute() and not root.is_relative_to(repo),'Fixture must be absolute and outside checkout'
assert not root.exists()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
minimum_free=5*1024**3
probe=root.parent
while not probe.exists():probe=probe.parent
free_at_admission=shutil.disk_usage(probe).free
assert free_at_admission>=minimum_free,'Need at least5GiB for full population, complete samples, source/exes and final archival'
root.mkdir();source=root/'source'
shutil.copyfile(__file__,root/'executed-producer.py')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def save(name,data):(root/name).write_text(json.dumps(data,indent=2)+'\n')
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
names=[n for n in subprocess.check_output(['git','ls-tree','-r','--name-only',revision],cwd=repo,text=True).splitlines() if n.endswith('.go') or n in ('go.mod','go.sum')]
subprocess.run(['git','worktree','add','--detach','--no-checkout',str(source),revision],cwd=repo,check=True)
subprocess.run(['git','sparse-checkout','set','--no-cone','--stdin'],cwd=source,input=''.join('/'+n+'\n' for n in names),text=True,check=True)
subprocess.run(['git','checkout','--detach',revision],cwd=source,check=True)
def inventory():
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 return {'revision':revision,'files':{n:sha(source/n) for n in names}}
before=inventory();save('source-before.json',before);save('disk-admission.json',dict(probe_path=str(probe),free_bytes=free_at_admission,minimum_free_bytes=minimum_free,scope='Launch estimate for this single finite full400k capacity fixture; not an ongoing reservation or 24h admission'))
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')}
env.update(GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',WF_MATRIX_BULK_CAPACITY_ROOT=str(root/'fixture'),WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
build=['go','test','-p=1','-buildvcs=true','-c','-o',str(root/'integration.test'),'./integration']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True)
assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info
save('binary.json',{'sha256':sha(root/'integration.test'),'build_info':info})
args=[str(root/'integration.test'),'-test.run=^TestMatrixBulkLatencyFull400kCapacity$','-test.v','-test.count=1','-test.timeout=100m']
save('commands.json',{'build':build,'test':args,'env':{k:v for k,v in env.items() if k.startswith('WF_') or k in ('GOMAXPROCS','GOMEMLIMIT','GOGC')},'source_capture_scope':'Selected Git Go/module source, retained isolated worktree and executable build metadata; not exhaustive external toolchain/compiler input capture'})
(root/'actual-containers').mkdir();seen=set();records=[];memory=[];native_usage=None
def poll_native():
 global native_usage
 if p.returncode is not None:return p.returncode
 pid,status,usage=os.wait4(p.pid,os.WNOHANG)
 if not pid:return None
 native_usage=usage
 p.returncode=os.waitstatus_to_exitcode(status)
 return p.returncode
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
 exe=Path(f'/proc/{p.pid}/exe')
 actual={'pid':p.pid,'source':revision,'sha256':sha(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'args':args,'stat':Path(f'/proc/{p.pid}/stat').read_text(),'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Fresh normal R5 full400k/4.8M valid activity latency capacity, complete independent timestamp oracle and 256 frozen point checks; original20s before/after and6m bulk limits; whole SDK lifetime Maxrss recorded; not live/default/matrix/24h or real-workflow/fault qualification'}
 assert actual['sha256']==sha(root/'integration.test');save('execution.json',actual)
 print('NATIVE_STARTED',p.pid,revision,flush=True)
 while poll_native() is None:
  try:
   status=Path(f'/proc/{p.pid}/status').read_text().splitlines()
   memory.append(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),pid=p.pid,values=[x for x in status if x.startswith(('VmRSS:','VmHWM:','VmSize:'))]))
   save('sdk-memory-observations.json',memory)
  except (FileNotFoundError,ProcessLookupError):pass
  ids=subprocess.check_output(['docker','ps','-q','--filter',f'name=js-wf-route-{p.pid}-'],text=True).split()
  for cid in ids:
   try:
    item=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0];pid=item['State']['Pid']
    if not pid or (cid,pid) in seen:continue
    path=Path(f'/proc/{pid}/exe');digest=sha(path)
    binary=root/'actual-containers'/digest
    if not binary.exists():shutil.copyfile(path,binary)
    assert sha(binary)==digest
    record={'container':item,'host_pid':pid,'sha256':digest,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_proc_build_info':subprocess.check_output(['go','version','-m',str(path)],text=True)}
    assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in record['actual_proc_build_info']
    records.append(record);seen.add((cid,pid));save('actual-containers/actual-servers.json',records)
   except (FileNotFoundError,ProcessLookupError,subprocess.CalledProcessError):continue
  time.sleep(1)
 code=p.wait();assert native_usage is not None
 actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),sdk_wait4_usage=dict(peak_rss_kib=native_usage.ru_maxrss,user_seconds=native_usage.ru_utime,system_seconds=native_usage.ru_stime,scope='Linux wait4 of this exact SDK child through exit, including test cleanup; excludes compiler/Git/other children and Docker servers'))
 save('execution.json',actual)
after=inventory();save('source-after.json',after);assert before==after
assert sha(root/'integration.test')==actual['sha256']
assert not Path(f'/proc/{p.pid}').exists()
for r in records:assert not Path('/proc/'+str(r['host_pid'])).exists()
assert sha(root/'executed-producer.py')==sha(Path(__file__))
print('NATIVE_FINISHED',code,flush=True)
raise SystemExit(code)
