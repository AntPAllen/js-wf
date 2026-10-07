import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,subprocess,hashlib,tempfile,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));sys.path.insert(0,'/tmp')
import fixture_archive
from storage_review_common import closure
root=Path('/tmp/js-wf-operator-connection-baseline-20261007');out=Path('/tmp/js-wf-operator-connection-baseline-independent-20261007')
read=lambda p:json.loads(p.read_text());r=read(root/'baseline.json');rev=r['source'];source=root/'source'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert r['expected_baseline_failure_proven'] is True and rev.startswith('3d04417')
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',root.name+'.service','--property=ActiveState,SubState,MainPID,ExecMainPID,ExecMainCode,ExecMainStatus,Result,InvocationID,Restart'],text=True).splitlines())
assert unit['ActiveState']=='active' and unit['SubState']=='exited' and unit['MainPID']=='0' and unit['ExecMainCode']=='1' and unit['ExecMainStatus']=='0' and unit['Result']=='success' and unit['Restart']=='no' and unit['InvocationID']=='253a7f16c7654ab1bd0dd01ee23ed896'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==rev and subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
# Stream every tracked Git blob; do not allocate the complete checkout in memory.
all_names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines()
flags={line[2:]:line[0] for line in subprocess.check_output(['git','ls-files','-v'],cwd=source,text=True).splitlines()}
missing=[n for n in all_names if not (source/n).is_file()]
assert all(flags[n]=='S' and not n.endswith(('.go','.py','.yml')) and n not in ('go.mod','go.sum') and not n.startswith('sim/testdata/') for n in missing)
names=[n for n in all_names if n not in missing]
with tempfile.TemporaryFile() as commands:
 commands.write(''.join(rev+':'+name+'\n' for name in names).encode());commands.seek(0)
 with subprocess.Popen(['git','cat-file','--batch'],cwd=repo,stdin=commands,stdout=subprocess.PIPE) as git:
  for name in names:
   header=git.stdout.readline().split();assert header[1]==b'blob';size=int(header[2]);expected=hashlib.sha256()
   while size:
    data=git.stdout.read(min(size,1<<20));assert data;expected.update(data);size-=len(data)
   assert git.stdout.read(1)==b'\n'
   assert sha(source/name)==expected.hexdigest(),name
  assert not git.stdout.read() and git.wait()==0
binary=root/'wf';assert sha(binary)==r['binary_sha256']
info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert info==r['build_info'] and 'vcs.revision='+rev in info and 'vcs.modified=false' in info and '-race=true' in info
records=r['records'];expected={(c,s,g) for c in ('project','tombstone-loop') for s in ('INFO','PONG') for g in ('SIGTERM','SIGINT')}
assert len(records)==8 and {(a['command'],a['stage'],a['signal']) for a in records}==expected and len({a['actual']['pid'] for a in records})==8
for record in records:
 actual=record['actual'];assert actual['admission']['stable_identity_observed_twice'] and actual['exe_sha256']==r['binary_sha256'] and actual['working_directory']==str(source)
 assert actual['args']==[str(binary),'-url',actual['args'][2],record['command']] and actual['args'][2].startswith('nats://127.0.0.1:')
 assert actual['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='')
 assert actual['stat'].split(') ',1)[1].split()[1]==unit['ExecMainPID'] and not Path('/proc',str(actual['pid'])).exists()
 assert record['exit_code']==(-15 if record['signal']=='SIGTERM' else -2) and record['stdout']==record['stderr']==''
 elapsed=(datetime.datetime.fromisoformat(record['joined_at'])-datetime.datetime.fromisoformat(record['sent_at'])).total_seconds();assert 0<=elapsed<3
 if record['stage']=='INFO':assert record['client_wire']==record['server_wire']==''
 else:
  assert record['server_wire'].startswith('INFO ')
  lines=record['client_wire'].split('\r\n');assert len(lines)==3 and lines[0].startswith('CONNECT ') and lines[1:]==['PING',''];json.loads(lines[0][8:])
meta=read(root.with_name(root.name+'-proof')/'archive-verification.json');inventory=read(root.with_name(root.name+'-proof')/'fixture-inventory.json')
assert sha(root.with_name(root.name+'-proof')/'fixture-inventory.json')==meta['inventory_sha256']
with root.with_suffix('.tar.gz').open('rb') as stream:declared,actual=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
report=dict(baseline_all_eight_failures_confirmed=True,source=rev,present_tracked_git_files_verified=len(names),inherited_sparse_archive_paths_omitted=missing,terminal_unit=unit,actual_children=8,full_archive=actual,archive_files=len(inventory['files']),fresh_closure=closure(root),scope='Previous official CLI code with every present tracked Git file verified (inherited sparse historical archive exclusions recorded) exits by signal at controlled INFO/PONG protocol fixtures; no NATS server, TLS/authentication/DNS or release qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('BASELINE_INDEPENDENTLY_CONFIRMED',len(names),len(inventory['files']),flush=True)
