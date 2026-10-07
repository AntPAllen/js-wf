import sys
sys.dont_write_bytecode=True
from pathlib import Path
import hashlib,importlib.util,io,json,re,shutil,subprocess
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-operator-leaf-wire-v2-20261007');out=Path('/tmp/js-wf-operator-leaf-wire-v2-independent-20261007')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive,operator_leaf_wire
from storage_review_common import closure
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-operator-domain-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
read=lambda p:json.loads(p.read_text());sdk=read(root/'actual-sdk.json');execution=read(root/'execution.json');rev=execution['source'];assert execution['exit_code']==0
unit=subprocess.check_output(['systemctl','show',root.name+'.service','-p','LoadState','-p','InvocationID','-p','ExecMainPID','-p','ExecMainCode','-p','ExecMainStatus','-p','MainPID','-p','SubState','-p','Result','-p','Restart'],text=True);fields=dict(line.split('=',1) for line in unit.splitlines())
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
log=(root/'native.log').read_text();qualification=native.verify_leaf_log(log);assert read(root/'row-review.json')==dict(qualification=qualification,rejection=None)
wire=operator_leaf_wire.validate(root/'stores');assert wire==read(root/'leaf-wire-review.json')
records=[read(p) for p in (root/'stores').rglob('standalone.process.json')];assert len(records)==23 and {p['pid'] for p in records}=={p['pid'] for p in wire['processes']}
assert len({p['exe_sha256'] for p in records})==1
for record in records:
 assert record['exe_sha256']==sha(Path(record['argv'][0])) and '-race=true' in record['build_info'] and 'vcs.revision='+rev in record['build_info'] and 'vcs.modified=false' in record['build_info']
 assert not Path('/proc',str(record['pid'])).exists() and f"operator standalone process domain=\"WFOPS\" pid={record['pid']} exit={record['exit_code']}" in log
for row in wire['processes']:
 assert f"operator child wire: pid={row['pid']} offline={str(row['offline']).lower()} connections={0 if row['offline'] else 1} truncated=false client_bytes={row['client_bytes']} server_bytes={row['server_bytes']}" in log
leaves=read(root/'leaf-processes.json');assert len(leaves)==1;leaf=leaves[0]
assert leaf['pid']==wire['leaf_pid'] and leaf['reaped'] is True and leaf['exit_code']==0 and sha(Path(leaf['exe']))==leaf['exe_sha256']
assert leaf['argv'][0]==leaf['exe'] and str(root) in leaf['argv'][-1] and 'v2.15.0' in leaf['build_info'] and not Path('/proc',str(leaf['pid'])).exists()
plugins=read(root/'plugins.json');assert len(plugins)==1 and plugins[0]['sha256']==sha(Path(plugins[0]['path'])) and '-race=true' in plugins[0]['build_info']
bad=[log.replace('local=WFEDGE','local=WFOPS'),log.replace('truncated=false','truncated=true',1),log.replace('processes=23','processes=22'),log.replace('--- PASS: '+native.LEAF_TESTS[0],'--- SKIP: '+native.LEAF_TESTS[0]),log+'DATA RACE']
for marker in ('operator child wire:','operator standalone process domain="WFOPS"','operator domain admitted node=2'):
 line=next(line for line in log.splitlines() if marker in line);bad.extend((log.replace(line,''),log.replace(line,line+'\n'+line)))
for altered in bad:
 assert altered!=log
 try:native.verify_leaf_log(altered)
 except AssertionError:pass
 else:raise AssertionError('actual native log mutation accepted')
meta_root=Path(str(root)+'-proof');meta=read(meta_root/'archive-verification.json');inventory=read(meta_root/'fixture-inventory.json')
assert sha(meta_root/'fixture-inventory.json')==meta['inventory_sha256']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
report=dict(accepted_healthy_operator_leaf=True,source=rev,selected_source_files=len(selected),actual_sdk=sdk,actual_children=23,actual_leaf=leaf,wire=wire,actual_log_mutations_rejected=len(bad),complete_archive=actual,archive_files=len(inventory['files']),fresh_closure=closure(root),terminal_unit=unit,scope='Complete original healthy packaged operator command suite through actual stock leaf/R3 embedded hubs; no daemon, SQL, injected fault, full graph/matrix or online-GC qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('OPERATOR_LEAF_NATIVE_ACCEPTED',rev,sdk['pid'],wire['domain_api_publications'],len(inventory['files']),len(bad),flush=True)
