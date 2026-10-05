from pathlib import Path
import subprocess,json,hashlib,os,time,datetime
root=Path(__file__).parent;repo=root/'source'
candidate=Path('/tmp/js-wf-nats-scheduler-server-candidate-v2-20261004/nats-server')
expected_candidate='ff7335643d02125ec50b8abaa0ca80fe4da36473aa14402c2dbcf029e63c9fe8'
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def write(n,x):
    p=root/n;t=p.with_name(p.name+'.tmp');t.write_text(json.dumps(x,indent=2)+'\n');os.replace(t,p)
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
assert sha(candidate)==expected_candidate
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
env=dict(os.environ,GOCACHE='/tmp/js-wf-go-build-cache-20261004',GOFLAGS='',CGO_ENABLED='0',GOMAXPROCS='2',GOMEMLIMIT='1GiB')
fmt='{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}'
listing=subprocess.check_output(['go','list','-deps','-f',fmt,'./cmd/wf-timer-volume'],cwd=repo,env=env,text=True);(root/'dependencies.txt').write_text(listing)
inputs=set()
for line in listing.splitlines():
    d,g,c=line.split('|');inputs.update(Path(d)/n for n in (g+' '+c).split())
inputs.update([repo/'go.mod',repo/'go.sum']);before={str(p):sha(p) for p in sorted(inputs)};captures={};local=0
for i,p in enumerate(sorted(inputs)):
    if p.is_relative_to(repo):
        assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+str(p.relative_to(repo))],cwd=repo)).hexdigest()==before[str(p)];local+=1
    out=root/'selected-source'/f'{i:04d}'/p.name;out.parent.mkdir(parents=True);out.write_bytes(p.read_bytes());assert sha(out)==before[str(p)];captures[str(p)]=str(out.relative_to(root))
write('source-before.json',before);write('captured-paths.json',captures)
binary=root/'wf-timer-volume'
build=['go','build','-p=1','-o',str(binary),'./cmd/wf-timer-volume']
run=[str(binary),'-root',str(root/'campaign'),'-server-candidate',str(candidate),'-count','1000000','-publishers','64','-horizon','24h','-lead','15m','-p99-limit','2s','-max-late','30s']
write('commands.json',dict(revision=revision,build=build,run=run,cwd=str(repo),candidate_sha256=expected_candidate,local_git_inputs=local,selected_inputs=len(inputs),environment={k:env[k] for k in ['GOCACHE','GOFLAGS','CGO_ENABLED','GOMAXPROCS','GOMEMLIMIT']},scope='Full million/24h diagnostic candidate comparison, not production release qualification. Original raw lateness and physical drain gates unchanged.'))
with (root/'build.log').open('wb') as f:subprocess.run(build,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
assert {str(p):sha(p) for p in inputs}==before
info=subprocess.check_output(['go','version','-m',str(binary)],text=True);(root/'binary-build-info.txt').write_text(info)
digest=sha(binary);start=time.monotonic();started=now()
with (root/'stdout.log').open('wb') as out,(root/'stderr.log').open('wb') as err:
    process=subprocess.Popen(run,cwd=repo,env=env,stdout=out,stderr=err)
    live=Path(f'/proc/{process.pid}/exe');assert sha(live)==digest
    live_info=subprocess.check_output(['go','version','-m',str(live)],text=True);assert live_info.splitlines()[1:]==info.splitlines()[1:]
    safe={k:v for k,v in (s.split('=',1) for s in Path(f'/proc/{process.pid}/environ').read_text().split('\0') if '=' in s) if k in ['GOCACHE','GOFLAGS','CGO_ENABLED','GOMAXPROCS','GOMEMLIMIT']}
    write('actual-program.json',dict(pid=process.pid,observed_at=now(),started_at=started,executable_link=os.readlink(live),sha256=sha(live),go_build_info=live_info,cmdline=Path(f'/proc/{process.pid}/cmdline').read_text().split('\0')[:-1],environment=safe,stat=Path(f'/proc/{process.pid}/stat').read_text(),live_bytes_and_all_build_fields_verified=True))
    proofs=[]
    limit=time.monotonic()+45
    while process.poll() is None and time.monotonic()<limit:
        proofs=[]
        for directory in Path('/proc').glob('[0-9]*'):
            try:
                stat_text=(directory/'stat').read_text();fields=stat_text.rpartition(')')[2].split()
                if int(fields[1])!=process.pid:continue
                command=(directory/'cmdline').read_text().split('\0')[:-1]
                if not command or command[0]!=str(root/'campaign/nats-server-candidate'):continue
                exe=directory/'exe';assert sha(exe)==expected_candidate
                actual_info=subprocess.check_output(['go','version','-m',str(exe)],text=True)
                expected_info=subprocess.check_output(['go','version','-m',str(candidate)],text=True)
                assert actual_info.splitlines()[1:]==expected_info.splitlines()[1:]
                proofs.append(dict(pid=int(directory.name),parent_pid=process.pid,observed_at=now(),cmdline=command,executable_link=os.readlink(exe),sha256=sha(exe),go_build_info=actual_info,stat=stat_text,live_bytes_and_all_build_fields_verified=True))
            except (FileNotFoundError,ProcessLookupError,PermissionError):continue
        if len(proofs)==3:break
        time.sleep(.2)
    write('actual-native-processes.json',proofs)
    write('launch.json',dict(observed_at=now(),revision=revision,pid=process.pid,program_live=process.poll() is None,confirmed_native_processes=len(proofs),actual_program_sha256=digest,actual_candidate_sha256=expected_candidate,requested_count=1000000,horizon='24h',lead='15m',p99_limit='2s',max_late='30s',terminal=False,qualifies_original_million=False,qualifies_24h=False,qualifies_production_server=False))
    status=process.wait()
after={str(p):sha(p) for p in inputs};write('source-after.json',after)
write('execution.json',dict(revision=revision,exit_code=status,started_at=started,finished_at=now(),wall_seconds=time.monotonic()-start,actual_binary_sha256=digest,binary_unchanged=sha(binary)==digest,source_before_after_identical=after==before,scope='Actual million/24h diagnostic candidate profile. Terminal exit is not independent gate qualification or adoption of the server candidate. Selected Go/module sources captured, not exhaustive assembly/embed/hermetic provenance.'))
assert sha(binary)==digest and after==before
raise SystemExit(status)
