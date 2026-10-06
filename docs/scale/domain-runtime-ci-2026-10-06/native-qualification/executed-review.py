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

root=Path('/tmp/js-wf-domain-runtime-ci-qualification-20261006');revision='1558ffc36bf6a0fd93286e95d17a07fdfa4d4af4'
unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show','js-wf-domain-runtime-ci-qualification-20261006.service','-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(ActiveState='inactive',MainPID='0',ExecMainStatus='0')
state=json.loads((root/'execution.json').read_text());assert state['status']=='accepted' and state['source']==revision
observed=closure(root)
spec=importlib.util.spec_from_file_location('controls',root/'source/scripts/run-domain-runtime-controls.py');controls=importlib.util.module_from_spec(spec);spec.loader.exec_module(controls)
reports={}
base=repo/'docs/scale/domain-runtime-ci-2026-10-06/native-qualification'
for case in ('read-controls','retirement-server-kill'):
 row=root/case;proofdir=root/(case+'-proof')
 native=json.loads((row/'execution.json').read_text());assert native['exit_code']==0 and native['source']==revision
 before=json.loads((row/'source-before.json').read_text());assert before==json.loads((row/'source-after.json').read_text())
 assert before==controls.source_inventory(revision)
 for name,h in before['files'].items():assert hashlib.sha256((row/'selected-source'/name).read_bytes()).hexdigest()==h
 actual=json.loads((row/'actual-sdk.json').read_text());binary=json.loads((row/'binary.json').read_text());commands=json.loads((row/'commands.json').read_text())
 assert actual['pid']>0 and not Path('/proc',str(actual['pid'])).exists()
 assert actual['exe']==str(row/'integration-race.test') and actual['args']==commands['run']
 assert actual['exe_sha256']==binary['sha256']==hashlib.sha256(Path(actual['exe']).read_bytes()).hexdigest()
 assert '-race=true' in binary['build_info']
 assert actual['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',**{k:str(row/'stores') for k in ('WF_RESULT_ABSENCE_ROOT','WF_SNAPSHOT_LEADER_ROOT','WF_CONTINUATION_RETIREMENT_PROCESS_ROOT')})
 qualified=controls.verify_log(case,(row/'native.log').read_text())
 if case=='retirement-server-kill':
  servers=json.loads((row/'actual-servers.json').read_text());assert len(servers)==12
  for server in servers:
   assert not Path('/proc',str(server['pid'])).exists()
   assert 'v2.15.0' in server['build_info'] and hashlib.sha256(Path(server['exe']).read_bytes()).hexdigest()==server['exe_sha256']
  import re
  log=(row/'native.log').read_text()
  held=re.findall(r'held=([0-9.]+)s ttl=12s',log);assert len(held)==1 and float(held[0])>=13
  epochs=re.findall(r'prior_epoch=(\d+) terminal_epoch=(\d+) records=(\d+)',log);assert epochs==[('54','76','11')]
 raw=root/(case+'.tar.gz');meta=json.loads((proofdir/'archive-verification.json').read_text());manifest=json.loads((proofdir/'fixture-inventory.json').read_text())
 with raw.open('rb') as stream:
  archived,fingerprint=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
 assert archived==manifest and fixture_archive.inventory(row)==manifest['files']
 out=base/case;out.mkdir(parents=True)
 for file in ('archive-verification.json','fixture-inventory.json'):shutil.copyfile(proofdir/file,out/file)
 shutil.copyfile(row/'native.log',out/'native.log')
 report=dict(execution=native,actual_sdk=actual,binary=binary,source_git_before_after_copied_equal=len(before['files']),qualified_log=qualified,proof=meta,archive=str(raw),closure=closure(row),scope='Exact committed CI runner native qualification at1558ffc. Full current row bytes and all archive members independently verified, actual SDK profile and Git source bound. Retirement pair observes twelve native servers and preserves original targets. Visible global closure observed separately; no store reopened, exhaustive lifetime/fullmatrix/24h or hosted CI acceptance claimed.')
 (out/'review.json').write_text(json.dumps(report,indent=2)+'\n');reports[case]=report
shutil.copyfile(__file__,base/'executed-review.py')
(base/'review.json').write_text(json.dumps(dict(unit=unit,execution=state,closure=observed,rows=reports),indent=2)+'\n')
print('EXACT_CI_RUNNER_NATIVE_QUALIFIED', {k:v['execution']['elapsed_seconds'] for k,v in reports.items()},flush=True)
