from pathlib import Path
import subprocess,json,hashlib,os,shutil,time,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-postgres-projection-fault-refreshed-50000-20261005');root.mkdir();(root/'originals').mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
container='js-wf-projection-pg-refreshed-50000-20261005';volume=container+'-data'
image=json.loads(subprocess.check_output(['docker','image','inspect','postgres:16-alpine']))[0]
(root/'postgres-image.json').write_text(json.dumps(image,indent=2)+'\n')
subprocess.run(['docker','volume','create',volume],check=True,stdout=subprocess.DEVNULL)
command=['docker','run','-d','--name',container,'--memory=1g','-p','127.0.0.1::5432','-e','POSTGRES_USER=workflow','-e','POSTGRES_DB=workflow','-e','POSTGRES_HOST_AUTH_METHOD=trust','-v',volume+':/var/lib/postgresql/data',image['Id']]
(root/'postgres-command.json').write_text(json.dumps(command,indent=2)+'\n');subprocess.run(command,check=True,stdout=subprocess.DEVNULL)
ready=time.monotonic()+60
while subprocess.run(['docker','exec',container,'pg_isready','-h','127.0.0.1','-U','workflow','-d','workflow'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
 if time.monotonic()>=ready:raise RuntimeError('PostgreSQL admission failed')
 time.sleep(.25)
inspect=json.loads(subprocess.check_output(['docker','inspect',container]))[0];pid=inspect['State']['Pid'];live=f'/proc/{pid}/exe'
version=subprocess.check_output(['docker','exec',container,'postgres','--version'],text=True).strip()
with (root/'actual-postgres').open('wb') as f:subprocess.run(['sudo','-n','cat',live],stdout=f,check=True)
digest=subprocess.check_output(['sudo','-n','sha256sum',live],text=True).split()[0];assert digest==sha(root/'actual-postgres')
(root/'actual-postgres.json').write_text(json.dumps({'container':inspect,'actual_host_pid':pid,'actual_executable_sha256':digest,'version':version},indent=2)+'\n')
port=inspect['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort'];dsn=f'postgres://workflow@127.0.0.1:{port}/workflow?sslmode=disable'
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for n,d in before.items():
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==d
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/n,p)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
fmt='{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
deps=subprocess.check_output(['go','list','-deps','-test','-f',fmt,'./integration'],cwd=repo,text=True);(root/'dependencies.txt').write_text(deps)
goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip());modules=Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip());inputs={};captured={}
for line in deps.splitlines():
 directory,*groups=line.split('|')
 for n in ' '.join(groups).split():
  p=(Path(directory)/n).resolve()
  if not p.is_file() or p.is_relative_to(repo):continue
  if str(p) in inputs:continue
  digest=sha(p);inputs[str(p)]=digest
  rel=Path('modules')/p.relative_to(modules) if p.is_relative_to(modules) else Path('toolchain')/p.relative_to(goroot) if p.is_relative_to(goroot) else Path('other')/str(p).lstrip('/')
  dest=root/'selected-external-source'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest);assert sha(dest)==digest;captured[str(p)]=str(dest.relative_to(root))
(root/'external-source-before.json').write_text(json.dumps(inputs,indent=2)+'\n');(root/'external-captured-paths.json').write_text(json.dumps(captured,indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')};env.update(GOCACHE='/tmp/js-wf-go-build-cache-20261004',GOMAXPROCS='2',GOMEMLIMIT='2GiB',WF_PROJECTION_POSTGRES_FAULT_ROOT=str(root/'originals'),WF_TEST_POSTGRES_DSN=dsn)
build=['go','test','-p=1','-buildvcs=true','-c','-o',str(root/'integration.test'),'./integration']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True);assert 'vcs.modified=false' in info and f'vcs.revision={revision}' in info and '-race=true' not in info
(root/'binary.json').write_text(json.dumps({'sha256':sha(root/'integration.test'),'build_info':info},indent=2)+'\n')
args=[str(root/'integration.test'),'-test.run=^TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocations$','-test.count=1','-test.v','-test.timeout=22m']
(root/'commands.json').write_text(json.dumps({'build':build,'test':args,'environment':{k:v for k,v in env.items() if k.startswith('WF_') or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE']}},indent=2)+'\n')
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT);exe=Path(f'/proc/{p.pid}/exe');actual={'pid':p.pid,'sha256':sha(exe),'exe':os.readlink(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'source':revision,'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_R3_servers_embedded_in_actual_sdk':True,'separate_server_process_hashes':False};assert actual['sha256']==sha(root/'integration.test');(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('NATIVE_STARTED',p.pid,revision,flush=True)
 code=p.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
after={n:sha(repo/n) for n in names};assert before==after;(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n')
external_after={n:sha(Path(n)) for n in inputs};assert inputs==external_after;(root/'external-source-after.json').write_text(json.dumps(external_after,indent=2)+'\n');shutil.copy2(__file__,root/'executed-producer.py');print('NATIVE_FINISHED',code,flush=True)
# Stop the owned SQL server only after the SDK exits, preserving final media.
with (root/'postgres-before-stop.log').open('wb') as f:subprocess.run(['docker','logs',container],stdout=f,stderr=subprocess.STDOUT,check=True)
subprocess.run(['docker','stop',container],check=True,stdout=subprocess.DEVNULL)
after=json.loads(subprocess.check_output(['docker','inspect',container]))[0];assert not after['State']['Running'] and after['State']['Pid']==0
(root/'postgres-stopped.json').write_text(json.dumps(after,indent=2)+'\n')
subprocess.run(['docker','cp',container+':/var/lib/postgresql/data',str(root/'postgres-stopped-data')],check=True)
subprocess.run(['sudo','-n','chown','-R',f'{os.getuid()}:{os.getgid()}',str(root/'postgres-stopped-data')],check=True)
mount=json.loads(subprocess.check_output(['docker','volume','inspect',volume]))[0]['Mountpoint']
script="import pathlib,sys,hashlib,json; r=pathlib.Path(sys.argv[1]); print(json.dumps({str(p.relative_to(r)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in r.rglob('*') if p.is_file()}))"
hashes=json.loads(subprocess.check_output(['sudo','-n','python3','-c',script,mount]))
for n,d in hashes.items():assert sha(root/'postgres-stopped-data'/n)==d
(root/'postgres-media-copy-verification.json').write_text(json.dumps({'volume':volume,'files':hashes,'all_closed_sql_media_bytes_match':True},indent=2)+'\n')
shutil.copy2(__file__,root/'executed-producer.py')
raise SystemExit(code)
