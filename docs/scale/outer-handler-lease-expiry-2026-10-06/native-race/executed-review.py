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
root=Path('/tmp/js-wf-outer-handler-lease-expiry-race-corrected-20261006')
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-outer-handler-lease-expiry-race-corrected-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
state=json.loads((root/'execution.json').read_text());assert state['status']=='native_pass' and state['exit_code']==0
assert state['source']=='fb1e515ae1712bda6acc36ad45f6eb5f179c835d'
for pid in (state['producer_pid'],state['actual_sdk_pid']):assert not Path('/proc',str(pid)).exists()
log=(root/'native.log').read_text();assert log.rstrip().endswith('PASS') and log.count('old owner fenced:')==2 and log.count('Reason:lease_heartbeat_lost Error:workflow lease was lost:')==2 and 'DATA RACE' not in log
assert log.count('production TTL12s / AckWait13s takeover=')==2
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(expected)==set(before['files']) and before['revision']==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')
for name,h in before['files'].items():
 assert hashlib.sha256((root/'source'/name).read_bytes()).hexdigest()==h
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',state['source']+':'+name],cwd=repo)).hexdigest()==h
binary=json.loads((root/'binary.json').read_text());actual=json.loads((root/'actual-sdk.json').read_text());commands=json.loads((root/'commands.json').read_text())
assert hashlib.sha256((root/'worker-race.test').read_bytes()).hexdigest()==binary['sha256']==actual['exe_sha256']
assert binary['race'] and '-race=true' in binary['build_info']
assert actual['args']==commands['run'] and actual['pid']==state['actual_sdk_pid']
assert actual['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',WF_OUTER_HANDLER_STORE_ROOT=str(root/'stores'))
import re
values=[float(x) for x in re.findall(r'takeover=([0-9.]+)s',log)]
assert len(values)==2 and all(12<x<30 for x in values)
assert log.count('key revision mismatch')==2 and 'wrong fencing evidence' not in log
assert log.count('--- PASS: TestOuterHandlerNativeLeaseExpiryAndLateAppend/')==2
raw_configs=[]
for mode,part in [('handler',53),('continuation',48)]:
 for node in range(3):
  base=root/'stores'/('TestOuterHandlerNativeLeaseExpiryAndLateAppend-'+mode)/('node-'+str(node))/'jetstream/$G/streams'
  paths=list((base/'WF_RUN/obs').glob('*/meta.inf'));assert len(paths)==1
  cfg=json.loads(paths[0].read_bytes())
  assert cfg['ack_wait']==13000000000 and cfg['max_deliver']==-1 and cfg['max_ack_pending']==1000 and cfg['filter_subject']=='wf.run.'+str(part)
  lease_config=json.loads((base/'KV_WF_LEASE/meta.inf').read_bytes())
  assert lease_config['num_replicas']==3 and lease_config['max_age']==12000000000
  raw_configs.append(dict(mode=mode,node=node,consumer=cfg,lease_stream=lease_config))
initial=closure(root)
out=repo/'docs/scale/outer-handler-lease-expiry-2026-10-06/native-race';raw=Path('/tmp/js-wf-outer-handler-lease-expiry-corrected-complete-20261006.tar.gz')
proof=fixture_archive.capture(root,raw,out);final=closure(root)
shutil.copyfile(__file__,out/'executed-review.py');shutil.copyfile(root/'native.log',out/'native.log')
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),execution=state,unit=unit,source_files_git_current_before_after_equal=len(expected),actual_sdk=actual,binary=binary,known_sdk_producer_pids_closed=[state['producer_pid'],state['actual_sdk_pid']],closure_before=initial,closure_after=final,proof=proof,takeover_seconds=values,raw_closed_replica_configs=raw_configs,scope='Complete focused R3 native race cases for both outer handler and named continuation accepted at executed fb1e515. Production TTL12/AckWait13 real server expiry, higher successor epoch/completion under30s, old heartbeat CAS fencing while handler held, worker shutdown, all-three public local physical queue drain, late SDK rejection/no effect and unchanged accepted journal plus integrity checker pass. Complete originals/source/actual SDK and closed replica configs retained. No stores reopened; no broader fault/fullmatrix/24h or exhaustive process lifetime qualification.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print('FOCUSED_NATIVE_LEASE_EXPIRY_ACCEPTED',len(expected),proof,flush=True)
