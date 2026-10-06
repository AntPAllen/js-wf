from pathlib import Path
import json,hashlib,subprocess,datetime,urllib.request,time,importlib.util
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-tier2-retained-serverclockminus-ten-minute-20261006')
read=lambda p:json.loads(p.read_text())
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=read(root/'execution.json');assert e['status']=='running' and sha(Path('/proc')/str(e['pid'])/'exe')==e['sha256']==sha(root/'integration.test')
source=read(root/'source-before.json');assert source['revision']==e['source']
for name,h in source['files'].items():assert sha(repo/name)==h==sha(root/'source'/name)
env=read(root/'commands.json')['environment'];assert (env['GOMAXPROCS'],env['GOMEMLIMIT'],env['WF_MATRIX_DURATION'])==('2','2GiB','10m')
servers=read(root/'observed-servers.json');assert len(servers)==3 and {v['node'] for v in servers}=={0,1,2}
fixture=root/'originals'/e['test'];mapping=read(fixture/'clock/skew-overlay.json')['Replace'];assert len(mapping)==1
original,patched=next(iter(mapping.items()));captured=read(root/'external-captured-paths.json');originalbytes=(root/captured[original]).read_bytes();marker=b'\tsec, nsec, mono := runtimeNow()\n';assert originalbytes.count(marker)==1
assert Path(patched)==fixture/'clock/skew-time.go' and Path(patched).read_bytes()==originalbytes.replace(marker,marker+b'\tsec += -60 // test-only wall-clock skew\n')
spec=importlib.util.spec_from_file_location('common',repo/'scripts/review-tier2-retained-workers.py');common=importlib.util.module_from_spec(spec);spec.loader.exec_module(common)
measurements=[]
for server in servers:
 argv=server['argv'];assert sha(Path('/proc')/str(server['pid'])/'exe')==server['actual_executable_sha256']==sha(root/server['captured'])
 executable=fixture/('nats-server-skewed' if server['node']==2 else 'nats-server');assert argv[0]==str(executable) and sha(executable)==server['actual_executable_sha256']
 port=argv[argv.index('-m')+1];before=time.time_ns()
 with urllib.request.urlopen('http://127.0.0.1:'+port+'/varz',timeout=2) as response:data=json.load(response)
 after=time.time_ns();offset=common.ns(data['now'])-(before+after)//2;want=-60_000_000_000 if server['node']==2 else 0;assert abs(offset-want)<=2_000_000_000
 measurements.append(dict(node=server['node'],server_id=data['server_id'],server_at=data['now'],before_ns=before,after_ns=after,offset_ns=offset))
assert len({v['server_id'] for v in measurements})==3
out=repo/'docs/scale/tier2-retained-server-clock-2026-10-06/negative-live-initial';out.mkdir(exist_ok=True)
(out/'observation.json').write_text(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=e['source'],actual_sdk_pid=e['pid'],actual_sdk_sha256=e['sha256'],source_inputs_unchanged=len(source['files']),normal_profile={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','WF_MATRIX_DURATION')},actual_servers=servers,exact_minus_sixty_second_overlay_verified=True,independent_pinned_clock_measurements=measurements,scope='Live initial actual process/source/profile/clock verification only; terminal native/copy/full-matrix qualification remains pending'),indent=2)+'\n')
print(json.dumps(measurements,indent=2))
