from pathlib import Path,PurePosixPath
import json,hashlib,subprocess,shutil,os,tarfile,urllib.request,datetime
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-million-candidate-24h-20261005')
snapshot=Path('/tmp/js-wf-million-candidate-launch-review-20261005');snapshot.mkdir(exist_ok=False)
out=repo/'docs/scale/million-candidate-24h-2026-10-05/launch'
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def copy(p,n):
    d=snapshot/n;d.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,d);assert sha(p)==sha(d)
command=json.loads((root/'commands.json').read_text());actual=json.loads((root/'actual-program.json').read_text());launch=json.loads((root/'launch.json').read_text())
assert command['revision']==launch['revision']=='68d69c04a0c83273c8b3a0a5fad72d3a352753bf'
assert command['run'][1:]==['-root',str(root/'campaign'),'-server-candidate','/tmp/js-wf-nats-scheduler-server-candidate-v2-20261004/nats-server','-count','1000000','-publishers','64','-horizon','24h','-lead','15m','-p99-limit','2s','-max-late','30s']
sdk=Path(f"/proc/{actual['pid']}/exe");assert sha(sdk)==sha(root/'wf-timer-volume')==actual['sha256']
info=subprocess.check_output(['go','version','-m',str(sdk)],text=True)
assert info.splitlines()[1:]==actual['go_build_info'].splitlines()[1:]==(root/'binary-build-info.txt').read_text().splitlines()[1:]
assert actual['environment']['GOMAXPROCS']=='2' and actual['environment']['GOMEMLIMIT']=='1GiB'
before=json.loads((root/'source-before.json').read_text());paths=json.loads((root/'captured-paths.json').read_text());assert set(paths)==set(before)
local=0
for n,digest in before.items():
    p=Path(n);assert sha(p)==sha(root/paths[n])==digest
    if p.is_relative_to(root/'source'):
        rel=str(p.relative_to(root/'source'));assert hashlib.sha256(subprocess.check_output(['git','show',command['revision']+':'+rel],cwd=repo)).hexdigest()==digest;local+=1
    copy(root/paths[n],paths[n])
assert local==command['local_git_inputs']==87 and len(before)==command['selected_inputs']==1697
native=json.loads((root/'actual-native-processes.json').read_text());assert len(native)==3
candidate_sha='ff7335643d02125ec50b8abaa0ca80fe4da36473aa14402c2dbcf029e63c9fe8'
monitor=[]
for p in native:
    exe=Path(f"/proc/{p['pid']}/exe");assert sha(exe)==p['sha256']==candidate_sha
    live_info=subprocess.check_output(['go','version','-m',str(exe)],text=True);assert live_info.splitlines()[1:]==p['go_build_info'].splitlines()[1:]
    args=p['cmdline'];port=int(args[args.index('-m')+1]);url=f'http://127.0.0.1:{port}/jsz?streams=true&config=true'
    with urllib.request.urlopen(url,timeout=3) as response:data=json.load(response)
    (snapshot/f"node-{p['pid']}-jsz.json").write_text(json.dumps(data,indent=2)+'\n');monitor.append(dict(pid=p['pid'],url=url,observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat()))
for n in ['producer.py','commands.json','dependencies.txt','captured-paths.json','source-before.json','actual-program.json','actual-native-processes.json','binary-build-info.txt','launch.json','build.log','wf-timer-volume']:copy(root/n,n)
copy(root/'campaign/nats-server-candidate','nats-server-candidate')
assert sha(snapshot/'nats-server-candidate')==candidate_sha
(snapshot/'report-at-review.json').write_bytes((root/'campaign/report.json').read_bytes())
report=json.loads((snapshot/'report-at-review.json').read_text());assert report['status']=='running' and report['scheduled_count']==1000000 and 0<=report['acknowledged_publishes']<=1000000 and report['revision']==command['revision'] and report['source_modified']=='false'
assert report['server_candidate_sha256']==candidate_sha and report['horizon']=='24h0m0s' and report['lead']=='15m0s' and report['P99Limit']=='2s' and report['MaxLateLimit']=='30s'
state=subprocess.check_output(['systemctl','--user','show','js-wf-million-candidate-24h-20261005.service','-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStatus'],text=True)
assert 'ActiveState=active' in state and 'SubState=running' in state;(snapshot/'service-state.txt').write_text(state)
copy(Path(__file__),'executed-launch-review.py')
review=dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=command['revision'],actual_program_pid=actual['pid'],actual_program_sha256=actual['sha256'],candidate_sha256=candidate_sha,actual_native_pids=[p['pid'] for p in native],all_live_bytes_and_build_fields_verified=True,selected_inputs=len(before),local_git_inputs=local,all_captured_source_bytes_verified=True,acknowledged_publishes=report['acknowledged_publishes'],full_publication_observed=report['acknowledged_publishes']==1000000,received_at_snapshot=report['unique_received'],first_due=report['FirstDue'],last_due=report['LastDue'],monitor=monitor,live_stores_archived=False,terminal=False,qualifies_original_million=False,qualifies_24h=False,qualifies_production_server=False)
(snapshot/'review.json').write_text(json.dumps(review,indent=2)+'\n')
files={str(p.relative_to(snapshot)):p for p in snapshot.rglob('*') if p.is_file()};ledger={n:dict(sha256=sha(p),bytes=p.stat().st_size) for n,p in files.items()}
archive=snapshot/'proof.tar.gz'
with tarfile.open(archive,'w:gz',compresslevel=6) as t:
    for n,p in sorted(files.items()):t.add(p,arcname=n,recursive=False)
seen=set()
with tarfile.open(archive,'r:gz') as t:
    for member in t:
        n=PurePosixPath(member.name);assert member.isfile() and not n.is_absolute() and '..' not in n.parts and n.as_posix()==member.name and member.name not in seen
        e=ledger[member.name];assert member.size==e['bytes'] and hashlib.file_digest(t.extractfile(member),'sha256').hexdigest()==e['sha256'];seen.add(member.name)
assert seen==set(ledger)
for n,p in files.items():assert sha(p)==ledger[n]['sha256']
out.mkdir(parents=True,exist_ok=False);parts=[];combined=hashlib.sha256()
with archive.open('rb') as f:
    for i,b in enumerate(iter(lambda:f.read(25*1024*1024),b'')):
        p=out/f'proof.tar.gz.part-{i:02d}';p.write_bytes(b);d=sha(p);assert d==hashlib.sha256(b).hexdigest();combined.update(p.read_bytes());parts.append(dict(path=p.name,bytes=len(b),sha256=d))
assert combined.hexdigest()==sha(archive)
(out/'manifest.json').write_text(json.dumps(dict(files=ledger,archive_bytes=archive.stat().st_size,archive_sha256=sha(archive),parts=parts,all_members_and_parts_readback_verified=True),indent=2)+'\n')
(out/'review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review),flush=True)
