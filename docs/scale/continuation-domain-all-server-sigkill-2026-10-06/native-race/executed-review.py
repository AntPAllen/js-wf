from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os,time
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)
root=Path('/tmp/js-wf-continuation-domain-all-server-sigkill-race-20261006')
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-continuation-domain-all-server-sigkill-race-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
state=json.loads((root/'execution.json').read_text());assert state['status']=='native_pass' and state['exit_code']==0
assert state['source']=='4b4719eec9f82502723ff4102659e096aeeda171'
for pid in (state['producer_pid'],state['actual_sdk_pid']):assert not Path('/proc',str(pid)).exists()
log=(root/'native.log').read_text();assert log.rstrip().endswith('PASS') and 'DATA RACE' not in log
assert log.count('--- PASS: TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndAllServerSIGKILL (')==1
assert log.count('native domain admitted node=')==3 and log.count('native domain healed node=')==3
assert log.count('retirement fresh manifest cut:')==3 and log.count('signal=killed')==3
assert log.count('retirement restarted node=')==3
assert 'reclaimed=2 calls=4 effects=3 terminals=2 shared_blob_retained=true fresh_initial_calls=2 manifest_drops=1' in log
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(expected)==set(before['files']) and before['revision']==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
for name,h in before['files'].items():
 assert hashlib.sha256((root/'source'/name).read_bytes()).hexdigest()==h
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',state['source']+':'+name],cwd=repo)).hexdigest()==h
binary=json.loads((root/'binary.json').read_text());actual=json.loads((root/'actual-sdk.json').read_text());commands=json.loads((root/'commands.json').read_text())
assert hashlib.sha256((root/'integration-race.test').read_bytes()).hexdigest()==binary['sha256']==actual['exe_sha256']
assert binary['race'] and '-race=true' in binary['build_info']
assert actual['args']==commands['run'] and actual['pid']==state['actual_sdk_pid']
assert actual['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',WF_CONTINUATION_RETIREMENT_PROCESS_ROOT=str(root/'stores'))
import re
admitted=re.findall(r'native domain admitted node=(\d) pid=(\d+) domain=WFRETIRE server_id=(\w+)',log)
healed=re.findall(r'native domain healed node=(\d) pid=(\d+) domain=WFRETIRE server_id=(\w+) elapsed=([0-9.]+)s',log)
assert len(admitted)==len(healed)==3
servers=json.loads((root/'actual-servers.json').read_text());assert len(servers)==6
for n,pid,sid in admitted:
 row=next(r for r in servers if r['pid']==int(pid));assert 'nats-server/v2' in row['build_info'] and 'v2.15.0' in row['build_info']
 replacement=next(r for r in healed if r[0]==n);assert replacement[1]!=pid and replacement[2]!=sid and float(replacement[3])<30
 next(r for r in servers if r['pid']==int(replacement[1]))
for row in servers:
 assert hashlib.sha256(Path(row['exe']).read_bytes()).hexdigest()==row['exe_sha256']
 assert not Path('/proc',str(row['pid'])).exists()
 assert '-c' in row['args'] and '-sd' in row['args']
for n in range(3):
 cfg=root/'stores'/'all-servers-true-lease-expiry-false-domain-WFRETIRE'/f'node-{n}-domain.conf'
 assert cfg.read_text()=='jetstream { domain: "WFRETIRE" }\n'
values=[float(r[3]) for r in healed]
raw_configs=dict(admitted=admitted,healed=healed,actual_servers=servers)
initial=closure(root)
out=repo/'docs/scale/continuation-domain-all-server-sigkill-2026-10-06/native-race';raw=Path('/tmp/js-wf-continuation-domain-all-server-sigkill-complete-20261006.tar.gz')
proof=fixture_archive.capture(root,raw,out);final=closure(root)
shutil.copyfile(__file__,out/'executed-review.py');shutil.copyfile(root/'native.log',out/'native.log')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),execution=state,unit=unit,source_files_git_current_before_after_equal=len(expected),actual_sdk=actual,binary=binary,known_sdk_producer_pids_closed=[state['producer_pid'],state['actual_sdk_pid']],closure_before=initial,closure_after=final,proof=proof,takeover_seconds=values,raw_closed_replica_configs=raw_configs,scope='Focused real domain retirement/reuse with manifest reply loss and all-three reaped SIGKILL accepted at executed 4b4719e. Original startup/scenario/30s whole-cut gates retained; exact effects, terminals, generation rejection, GC reachability and all-peer checks pass. Actual native SDK and six server incarnations observed, complete closed originals preserved. No store reopened or other domain/legacy/fullmatrix/24h qualification.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print('DOMAIN_ALL_SERVER_SIGKILL_ACCEPTED',len(expected),proof,flush=True)
