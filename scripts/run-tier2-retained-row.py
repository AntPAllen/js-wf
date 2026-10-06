#!/usr/bin/env python3
from pathlib import Path
import subprocess,json,hashlib,os,shutil,time,datetime,argparse,signal,traceback
from matrix_process_observer import observe_servers
from matrix_worker_observer import observe_workers
from retained_input_cache import retain
from tier2_retained_profiles import ROWS, CLOCK_ROWS, sdk_timeout, capture_legacy_server

parser=argparse.ArgumentParser(description='Retain one original Tier2 row, actual SDK, selected source, observed NATS executables and closed process stores. Full13x200 gate remains separate.')
parser.add_argument('--root',type=Path,required=True)
parser.add_argument('--row',choices=ROWS,required=True)
parser.add_argument('--seed',type=int,default=1)
parser.add_argument('--duration',choices=['10m','35s'],default='10m',help='35s smoke never qualifies sustained duration')
parser.add_argument('--old-server',type=Path,help='Absolute NATS 2.11.17 executable for the upgrade row; retained before use')
parser.add_argument('--partition-diagnostics',action='store_true',help='Capture live per-peer public Raft/stream status alongside the original partition fault; gates unchanged')
parser.add_argument('--race',action='store_true',help='explicit race profile; default preserves standard Tier2 normal execution')
parser.add_argument('--prepare-only',action='store_true',help='Capture selected source and compile SDK without starting native tests')
parser.add_argument('--prepared-inputs',type=Path,help='Verified prepare-only fixture at this exact clean revision')
parser.add_argument('--input-cache',type=Path,help='Campaign-owned immutable byte cache; broker media are never shared')
args=parser.parse_args()
repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
root=args.root
if not root.is_absolute() or root.resolve().is_relative_to(repo) or not -(2**63)<=args.seed<2**63:
 parser.error('require an absolute evidence root outside the checkout and an int64 seed')
if args.partition_diagnostics and args.row!='partition':parser.error('--partition-diagnostics applies only to partition')
if (args.row=='upgrade') != (args.old_server is not None):parser.error('--old-server is required only for upgrade')
root=root.resolve()
cache=args.input_cache
if cache is not None and (not cache.is_absolute() or cache.resolve().is_relative_to(repo) or cache.is_symlink()):parser.error('input cache must be absolute outside checkout')
if args.prepare_only and args.prepared_inputs:parser.error('prepare-only cannot reuse another preparation')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
root.mkdir(mode=0o700);(root/'originals').mkdir(mode=0o700)
producer_bytes=Path(__file__).read_bytes();producer_path=str(Path(__file__).resolve().relative_to(repo))
assert producer_bytes==subprocess.check_output(['git','show',revision+':'+producer_path],cwd=repo)
observer=Path(__file__).with_name('matrix_process_observer.py');observer_bytes=observer.read_bytes();assert observer_bytes==subprocess.check_output(['git','show',revision+':scripts/matrix_process_observer.py'],cwd=repo);(root/'matrix_process_observer.py').write_bytes(observer_bytes)
worker_observer=Path(__file__).with_name('matrix_worker_observer.py');worker_observer_bytes=worker_observer.read_bytes();assert worker_observer_bytes==subprocess.check_output(['git','show',revision+':scripts/matrix_worker_observer.py'],cwd=repo);(root/'matrix_worker_observer.py').write_bytes(worker_observer_bytes)
profiles=Path(__file__).with_name('tier2_retained_profiles.py');profiles_bytes=profiles.read_bytes();assert profiles_bytes==subprocess.check_output(['git','show',revision+':scripts/tier2_retained_profiles.py'],cwd=repo);(root/'tier2_retained_profiles.py').write_bytes(profiles_bytes)
cache_module=Path(__file__).with_name('retained_input_cache.py');cache_bytes=cache_module.read_bytes();assert cache_bytes==subprocess.check_output(['git','show',revision+':scripts/retained_input_cache.py'],cwd=repo);(root/'retained_input_cache.py').write_bytes(cache_bytes)
(root/'producer-source.json').write_text(json.dumps({'revision':revision,'path':producer_path,'sha256':hashlib.sha256(producer_bytes).hexdigest()},indent=2)+'\n')
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for n,d in before.items():
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==d
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);retain(repo/n,p,cache)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
fmt='{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
deps=subprocess.check_output(['go','list','-deps','-test','-f',fmt,'./integration'],cwd=repo,text=True);(root/'dependencies.txt').write_text(deps)
goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip());modules=Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip());gocache=Path(subprocess.check_output(['go','env','GOCACHE'],text=True).strip()).resolve();inputs={};captured={};generated={}
for line in deps.splitlines():
 directory,*groups=line.split('|')
 for n in ' '.join(groups).split():
  p=(Path(directory)/n).resolve()
  if not p.is_file() or p.is_relative_to(repo):continue
  if str(p) in inputs or str(p) in generated:continue
  digest=sha(p)
  rel=Path('modules')/p.relative_to(modules) if p.is_relative_to(modules) else Path('toolchain')/p.relative_to(goroot) if p.is_relative_to(goroot) else Path('other')/str(p).lstrip('/')
  dest=root/'selected-external-source'/rel;dest.parent.mkdir(parents=True,exist_ok=True);retain(p,dest,cache);assert sha(dest)==digest
  if p.is_relative_to(gocache) and b"// Code generated by 'go test'. DO NOT EDIT." in p.read_bytes()[:120]:
   generated[str(p)]={'sha256':digest,'captured':str(dest.relative_to(root)),'scope':'Go-generated test main, retained before build; source cache path may disappear.'}
  else:
   inputs[str(p)]=digest;captured[str(p)]=str(dest.relative_to(root))
(root/'generated-inputs.json').write_text(json.dumps(generated,indent=2)+'\n')
(root/'external-source-before.json').write_text(json.dumps(inputs,indent=2)+'\n');(root/'external-captured-paths.json').write_text(json.dumps(captured,indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith(('WF_','MATRIX_','TIER3_MATRIX_'))};env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB',WF_MATRIX_CHAOS='1',WF_MATRIX_OPERATION_TIMINGS='1',WF_MATRIX_DURATION=args.duration,WF_MATRIX_PROCESS_ROOT=str(root/'originals'),MATRIX_ARTIFACT_PREFIX=str(root/('matrix-'+args.row+'-'+str(args.seed))),FAULT_SEED=str(args.seed))
row=args.row;duration=args.duration;race=args.race;test=ROWS[row]
if args.partition_diagnostics:env['WF_MATRIX_PARTITION_DIAGNOSTICS']='1'
if row=='blockdisk':env['WF_BLOCK_DISK']='1'
if row=='upgrade':env['WF_NATS_SERVER_BIN']=str(capture_legacy_server(args.old_server,root))
selection='^'+test+'$'
build=['go','test']+(['-race'] if race else [])+['-p=1','-buildvcs=true','-c','-o',str(root/'integration.test'),'./integration']
if args.prepared_inputs is None:
 with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
else:
 prepared=args.prepared_inputs.resolve();assert args.prepared_inputs.is_absolute() and not args.prepared_inputs.is_symlink() and prepared.is_dir()
 proof=json.loads((prepared/'preparation.json').read_text());assert proof['source']==revision and proof['race']==race and proof['status']=='prepared'
 assert json.loads((prepared/'source-before.json').read_text())==json.loads((prepared/'source-after.json').read_text())=={'revision':revision,'files':before}
 assert json.loads((prepared/'external-source-before.json').read_text())==json.loads((prepared/'external-source-after.json').read_text())==inputs
 assert sha(prepared/'integration.test')==proof['sha256']
 retain(prepared/'integration.test',root/'integration.test',cache,executable=True)
 (root/'build.log').write_text('Reused exact prepared SDK; no per-seed compile.\n')
 (root/'prepared-inputs.json').write_text(json.dumps({'root':str(prepared),'preparation':proof,'preparation_sha256':sha(prepared/'preparation.json'),'source_before_sha256':sha(prepared/'source-before.json'),'external_before_sha256':sha(prepared/'external-source-before.json')},indent=2)+'\n')
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True);assert 'vcs.modified=false' in info and f'vcs.revision={revision}' in info and (('-race=true' in info)==race)
(root/'binary.json').write_text(json.dumps({'sha256':sha(root/'integration.test'),'build_info':info},indent=2)+'\n')
if args.prepare_only:
 assert before=={n:sha(repo/n) for n in names} and inputs=={n:sha(Path(n)) for n in inputs}
 (root/'source-after.json').write_text((root/'source-before.json').read_text())
 (root/'external-source-after.json').write_text((root/'external-source-before.json').read_text())
 (root/'preparation.json').write_text(json.dumps({'source':revision,'race':race,'status':'prepared','sha256':sha(root/'integration.test'),'build_info':info,'build':build,'scope':'Source-bound compilation only; no native execution or acceptance.'},indent=2)+'\n')
 shutil.copy2(__file__,root/'executed-producer.py');print('PREPARED',revision,flush=True);raise SystemExit(0)

prepared_input_root=str(args.prepared_inputs) if args.prepared_inputs is not None else None
args=[str(root/'integration.test'),'-test.run='+selection,'-test.count=1','-test.v','-test.timeout='+sdk_timeout(row)]
(root/'commands.json').write_text(json.dumps({'build':build if prepared_input_root is None else None,'prepared_input_root':prepared_input_root,'test_command':args,'row':row,'test':test,'duration':duration,'race':race,'sustained_ten_minutes':duration=='10m','environment':{k:v for k,v in env.items() if k.startswith(('WF_','MATRIX_')) or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE','FAULT_SEED']}},indent=2)+'\n')
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=repo/'integration',env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try:
  exe=Path(f'/proc/{p.pid}/exe');actual={'pid':p.pid,'sha256':sha(exe),'exe':os.readlink(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'source':revision,'row':row,'test':test,'duration':duration,'race':race,'sustained_ten_minutes':duration=='10m','status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'start_ticks':Path(f'/proc/{p.pid}/stat').read_text().rsplit(')',1)[1].split()[19],'actual_argv':Path(f'/proc/{p.pid}/cmdline').read_bytes().decode().rstrip('\0').split('\0'),'server_observation_scope':'Periodic owned NATS descendants; point-in-time /proc bytes, not exhaustive process lifetime coverage.'};actual_environment=dict(item.split(b'=',1) for item in Path(f'/proc/{p.pid}/environ').read_bytes().split(b'\0') if b'=' in item);actual['environment']={k:os.fsdecode(actual_environment[k.encode()]) for k in env if k.startswith(('WF_','MATRIX_')) or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE','FAULT_SEED']};assert actual['environment']==json.loads((root/'commands.json').read_text())['environment'];assert actual['sha256']==sha(root/'integration.test');(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('NATIVE_STARTED',p.pid,revision,flush=True)
 except BaseException:
  (root/'producer-error.txt').write_text(traceback.format_exc())
  os.killpg(p.pid,signal.SIGTERM)
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:
   os.killpg(p.pid,signal.SIGKILL);p.wait()
  raise

 seen=set();servers=[];workers_seen=set();workers=[];observation_errors=[]
 try:
  while p.poll() is None:
   try:
    observe_servers(p.pid,root,seen,servers,cache)
    observe_workers(p.pid,root,workers_seen,workers,cache)
   except Exception:
    observation_errors.append({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'error':traceback.format_exc()})
    (root/'observer-errors.json').write_text(json.dumps(observation_errors,indent=2)+'\n')
   (root/'observed-servers.json').write_text(json.dumps(servers,indent=2)+'\n')
   (root/'observed-workers.json').write_text(json.dumps(workers,indent=2)+'\n')
   try:p.wait(timeout=0.25)
   except subprocess.TimeoutExpired:pass
  code=p.wait()
 except BaseException:
  (root/'producer-error.txt').write_text(traceback.format_exc())
  os.killpg(p.pid,signal.SIGTERM)
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:
   os.killpg(p.pid,signal.SIGKILL);p.wait()
  actual.update(status='interrupted',exit_code=p.returncode,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
  (root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
  raise
 actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),server_observer_errors=len(observation_errors))
 (root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')

after={n:sha(repo/n) for n in names};assert before==after;(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n')
assert all(sha(root/d['captured'])==d['sha256'] for d in generated.values())
external_after={n:sha(Path(n)) for n in inputs};assert inputs==external_after;(root/'external-source-after.json').write_text(json.dumps(external_after,indent=2)+'\n');assert Path(__file__).read_bytes()==producer_bytes and observer.read_bytes()==observer_bytes and worker_observer.read_bytes()==worker_observer_bytes and profiles.read_bytes()==profiles_bytes and cache_module.read_bytes()==cache_bytes;shutil.copy2(__file__,root/'executed-producer.py');print('NATIVE_FINISHED',code,flush=True)

if args.partition_diagnostics:env['WF_MATRIX_PARTITION_DIAGNOSTICS']='1'
if row=='blockdisk':
 images=list((root/'originals').glob('*/wf-block-*/backing.img'))
 media=[{'path':str(p.relative_to(root)),'bytes':p.stat().st_size,'sha256':sha(p)} for p in images]
 (root/'block-media.json').write_text(json.dumps({'images':media,'scope':'Closed original raw filesystem images; copied mount review remains separate.'},indent=2)+'\n')
 if code==0:
  assert len(media)==1 and media[0]['bytes']==512*1024*1024, 'passing block row must retain its closed backing image'
checker=repo/'scripts/check-matrix-result.py'
checker_bytes=subprocess.check_output(['git','show',revision+':scripts/check-matrix-result.py'],cwd=repo)
assert checker.read_bytes()==checker_bytes
(root/'executed-checker.py').write_bytes(checker_bytes)
with (root/'native.log').open('rb') as native, (root/'converted-events.jsonl').open('wb') as converted:
 subprocess.run(['go','tool','test2json','-t','-p','js-wf/integration'],stdin=native,stdout=converted,check=True)
with (root/'acceptance.log').open('w') as log:
 acceptance=subprocess.run(['python3',str(root/'executed-checker.py'),str(root/'converted-events.jsonl'),test,duration],stdout=log,stderr=subprocess.STDOUT)
(root/'acceptance.json').write_text(json.dumps({'exit_code':acceptance.returncode,'native_exit_code':code,'duration':duration,'row':row,'source':revision,'server_observer_errors':len(observation_errors),'scope':'Native duration acceptance only; independent fault/history/store review still required.'},indent=2)+'\n')
raise SystemExit(code or acceptance.returncode or bool(observation_errors))
