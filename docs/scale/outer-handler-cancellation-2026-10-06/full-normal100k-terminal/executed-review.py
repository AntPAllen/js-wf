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
root=Path('/tmp/js-wf-tier1-handler-boundary-normal100k-20261006');run=root/'run';source=root/'source'
# Review the same original producer only after its unit exits; never restart it.
while subprocess.check_output(['systemctl','--user','show','js-wf-tier1-handler-boundary-normal100k-20261006.service','-p','ActiveState','--value'],text=True).strip() in ('active','activating','deactivating'):
 print('WAITING_FOR_ORIGINAL_NORMAL100K_TERMINAL',datetime.datetime.now(datetime.timezone.utc).isoformat(),flush=True)
 time.sleep(30)
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-tier1-handler-boundary-normal100k-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0'),unit
state=json.loads((root/'execution.json').read_text());assert state['status']=='passed' and state['exit_code']==0
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
launch=json.loads(subprocess.check_output(['git','show',head+':docs/scale/outer-handler-cancellation-2026-10-06/full-normal100k-launch/launch.json'],cwd=repo))
for pid in [3308963,state['producer_pid'],launch['actual_sdk_pid']]:assert not Path('/proc',str(pid)).exists()
assert state['source']==launch['execution']['source']=='4f9303953eeac96fc7dd0d825168e1fa9ea9f1b6'
assert state['configured_original_timeout']=='300m' and state['seeds']==100000 and state['race'] is False
binary=json.loads((run/'binary.json').read_text());assert binary==launch['binary']
assert hashlib.sha256((run/'sim.test').read_bytes()).hexdigest()==binary['binary_sha256']
assert binary['race_instrumented'] is False and '-race=true' not in binary['build_info']
assert binary['environment']==launch['actual_profile']==dict(GOMEMLIMIT='512MiB',GOMAXPROCS='2',SIM_SEEDS='100000',SIM_COVERAGE_SUMMARY='1')
before=json.loads((run/'source-before.json').read_text());assert before==json.loads((run/'source-after.json').read_text()) and before['revision']==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
assert set(expected)==set(before['files'])
for name,sha in before['files'].items():
 assert hashlib.sha256((source/name).read_bytes()).hexdigest()==sha
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',state['source']+':'+name],cwd=repo)).hexdigest()==sha
spec=importlib.util.spec_from_file_location('checker',repo/'scripts/check-tier1-suite.py');checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
events=[json.loads(line) for line in (run/'tier1-events.jsonl').read_text().splitlines() if line.strip()]
report=checker.check(events,(run/'tier1-inventory.txt').read_text(),100000,state['source'],(run/'tier1-regression-inventory.txt').read_text(),(run/'tier1-seeded-inventory.txt').read_text())
report['events_sha256']=hashlib.sha256((run/'tier1-events.jsonl').read_bytes()).hexdigest()
assert report==json.loads((run/'tier1-result.json').read_text())
assert report['per_workload_seed_proof']['workloads']==122 and report['per_workload_seed_proof']['completed_bodies']==12200000 and report['pinned_regressions_pass']==392
initial=closure(root)
out=repo/'docs/scale/outer-handler-cancellation-2026-10-06/full-normal100k-terminal';raw=Path('/tmp/js-wf-tier1-handler-boundary-normal100k-complete-20261006.tar.gz')
proof=fixture_archive.capture(root,raw,out);final=closure(root)
shutil.copyfile(__file__,out/'executed-review.py')
result=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),unit=unit,execution=state,source_files_exactly_bound_to_git=len(before['files']),source_before_after_current_git_equal=True,actual_sdk_pid=launch['actual_sdk_pid'],known_sdk_producer_supervisor_pids_closed=[3308963,state['producer_pid'],launch['actual_sdk_pid']],retained_binary_equals_observed_binary=True,actual_profile=launch['actual_profile'],suite=report,whole_suite_elapsed=(run/'tier1-time.txt').read_text().strip(),complete_archive=proof,closure_before=initial,closure_after=final,scope='Complete original122-workload100000-seed normal suite and392 pins accepted at executed4f93039 after independent exact inventory/source/binary/closure checks. Broader native fault combinations/full real-cluster matrices/million physical drain/actual24h remain open. Simulation does not model NATS disk/Raft internals.')
(out/'independent-review.json').write_text(json.dumps(result,indent=2)+'\n');print('COMPLETE_NORMAL100K_REVIEW_AND_ARCHIVE_VERIFIED',report['per_workload_seed_proof']['completed_bodies'],report['pinned_regressions_pass'],flush=True)
