import sys
sys.dont_write_bytecode=True
from pathlib import Path
import hashlib,importlib.util,io,json,re,shutil,subprocess
repo=Path('/home/exedev/js-wf');root=Path(sys.argv[1]);out=Path(sys.argv[2])
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
from storage_review_common import closure
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-operator-domain-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
read=lambda p:json.loads(p.read_text());sdk=read(root/'actual-sdk.json');execution=read(root/'execution.json');rev=execution['source'];assert execution['exit_code']==0
unit=subprocess.check_output(['systemctl','--user','show',root.name+'.service','-p','LoadState','-p','InvocationID','-p','ExecMainPID','-p','ExecMainCode','-p','ExecMainStatus','-p','MainPID','-p','SubState','-p','Result','-p','Restart'],text=True);fields=dict(line.split('=',1) for line in unit.splitlines())
assert fields['LoadState']=='loaded' and fields['ExecMainCode']=='1' and fields['ExecMainStatus']=='0' and fields['MainPID']=='0' and fields['SubState']=='exited' and fields['Result']=='success' and fields['Restart']=='no'
assert sdk['stat'].split(') ',1)[1].split()[1]==fields['ExecMainPID'] and sdk['admission']['stable_identity_observed_twice']
before=read(root/'source-before.json');assert before==read(root/'source-after.json') and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in selected).encode()))
for name in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(data).hexdigest()==before['files'][name]==hashlib.sha256((root/'selected-source'/name).read_bytes()).hexdigest()
assert not stream.read()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
binary=read(root/'binary.json');commands=read(root/'commands.json');assert sdk['args']==commands['run'] and sdk['working_directory']==str(repo/'cmd/wf')
assert sdk['exe_sha256']==binary['sha256']==sha(root/'operator-race.test') and sdk['exe']==str(root/'operator-race.test')
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='',WF_OPERATOR_TEST_ROOT=str(root/'stores'),WF_OPERATOR_STANDALONE='1')
assert '-race=true' in binary['build_info'] and 'vcs.revision='+rev in binary['build_info'] and 'vcs.modified=false' in binary['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in binary['build_info']
assert not Path('/proc',str(sdk['pid'])).exists()
log=(root/'native.log').read_text()
assert log.rstrip().endswith('PASS') and not any(v in log for v in ('DATA RACE','--- FAIL:','--- SKIP:'))
if 'signals' in root.name:
 records=[read(p) for p in (root/'stores').rglob('connection.process.json')]
 expected={(c,s,g) for c in ('project','tombstone-loop') for s in ('INFO','PONG') for g in ('terminated','interrupt')}
 row=read(root/'row-review.json');assert row['rejection'] is None and row['qualification']['actual_standalone_processes']==8 and row['qualification']['qualified_boundaries']==[list(x) for x in sorted(expected)]
 assert len(records)==8 and len({r['pid'] for r in records})==8
 assert {(r['command'],r['stage'],r['signal']) for r in records}==expected
 assert records==read(root/'connection-processes.json') or sorted(records,key=lambda r:r['pid'])==sorted(read(root/'connection-processes.json'),key=lambda r:r['pid'])
 assert re.findall(r'^--- PASS: (\w+) \(',log,re.M)==['TestOperatorStandaloneConnectionSignals']
 logged=re.findall(r'operator connection signal command=(project|tombstone-loop) stage=(INFO|PONG) signal=(terminated|interrupt) exit=0 elapsed=\S+ pid=(\d+)',log)
 assert len(logged)==8 and {(c,s,g,int(p)) for c,s,g,p in logged}=={(r['command'],r['stage'],r['signal'],r['pid']) for r in records}
 from datetime import datetime
 for record in records:
  assert record['scenario_passed'] is True and type(record['exit_code']) is int and record['exit_code']==0
  assert record['argv']==[record['binary'],*record['args']] and record['args'][-1]==record['command']
  assert record['exe_sha256']==sha(Path(record['binary'])) and '-race=true' in record['build_info'] and 'vcs.revision='+rev in record['build_info'] and 'vcs.modified=false' in record['build_info']
  assert not Path('/proc',str(record['pid'])).exists() and record['stat'].startswith(str(record['pid'])+' ')
  a,b,c=[datetime.fromisoformat(record[k]) for k in ('admitted_at','signal_sent_at','joined_at')]
  assert a<=b<=c and (c-b).total_seconds()<3
  if record['stage']=='INFO':assert record['client_wire']==record['server_wire']==''
  else:
   assert record['server_wire'].startswith('INFO ')
   lines=record['client_wire'].split('\r\n');assert len(lines)==3 and lines[0].startswith('CONNECT ') and lines[1:]==['PING',''];json.loads(lines[0][8:])
 scope='Actual packaged CLI controlled INFO/PONG handshakes only; no native NATS, TLS/authentication/DNS fault or complete release qualification.'
else:
 result=native.verify_daemon_log(log)
 assert read(root/'row-review.json')==dict(qualification=result,rejection=None)
 records=read(root/'daemon-processes.json');assert len(records)==8 and len({r['pid'] for r in records})==8
 for r in records:
  assert r['exe_sha256']==sha(root/'operator-race.test') and r['argv']==[str(root/'operator-race.test'),'-test.run=^TestOperatorDaemonProcessHelper$'] and not Path('/proc',str(r['pid'])).exists()
 scope=result['scope']
assert read(root/'plugins.json')==[]
meta_root=Path(str(root)+'-proof');meta=read(meta_root/'archive-verification.json');inventory=read(meta_root/'fixture-inventory.json')
assert sha(meta_root/'fixture-inventory.json')==meta['inventory_sha256']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
report=dict(accepted=True,source=rev,selected_source_files=len(selected),actual_sdk=sdk,actual_children=len(records),complete_archive=actual,archive_files=len(inventory['files']),fresh_closure=closure(root),terminal_unit=unit,scope=scope)
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('OPERATOR_NATIVE_ACCEPTED',rev,sdk['pid'],len(records),len(inventory['files']),flush=True)
