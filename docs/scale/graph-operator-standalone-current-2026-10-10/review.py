#!/usr/bin/env python3
"""Review compiled operators, frozen Git, real exits and exclusive leaf wire."""
import gzip,hashlib,io,json,re,shutil,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
here=Path(__file__).resolve().parent;repo=here.parents[2]
state=json.loads((here/'state.json').read_text());root=Path(state['root']);checkout=Path(state['checkout'])
props=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',state['unit'],
    '-p','LoadState','-p','MainPID','-p','InvocationID','-p','RemainAfterExit','-p','ExecMainStatus','-p','ExecMainExitTimestamp'],text=True).splitlines())
assert props['LoadState']=='loaded' and props['InvocationID']==state['invocation'] and props['RemainAfterExit']=='yes'
assert state['terminal'] and state['child_exit']==0 and props['MainPID']=='0'
assert props['ExecMainStatus']=='0' and props['ExecMainExitTimestamp'] and state['inputs_unchanged']
assert state['command']==['/usr/local/bin/go','test','-race','./cmd/wf','-run','^TestOperatorStandalone','-count=1','-v','-timeout=15m']
assert state['configuration']=={'WF_OPERATOR_STANDALONE':'1','WF_OPERATOR_TEST_ROOT':str(root),'GOWORK':'off','GOFLAGS':''}
before=json.loads(gzip.decompress((here/'source-before.json.gz').read_bytes()))
assert before==json.loads(gzip.decompress((here/'source-after.json.gz').read_bytes()))
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==state['source']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',state['source']],cwd=repo,text=True).splitlines()
expected={n for n in names if n.split('/')[0] not in ('docs','scripts','.github') and (n.endswith('.go') or n in ('go.mod','go.sum') or '/testdata/' in n)}
assert set(before)==expected
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(state['source']+':'+n+'\n' for n in before).encode()))
for name,digest in before.items():
    header=stream.readline().split();assert header[1]==b'blob'
    data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
    assert hashlib.sha256(data).hexdigest()==digest==hashlib.sha256((checkout/name).read_bytes()).hexdigest(),name
log=(here/'race.log').read_text()
assert hashlib.sha256(log.encode()).hexdigest()==state['log_sha256']
assert re.search(r'^ok\s+js-wf/cmd/wf\s+',log,re.M)
assert not any(x in log for x in ('--- FAIL:','--- SKIP:','WARNING: DATA RACE','panic:'))
tests=('TestOperatorStandaloneConnectionSignals','TestOperatorStandaloneDaemonSignalsThroughLeaf',
    'TestOperatorStandaloneCommandsThroughLeaf','TestOperatorStandaloneCommands','TestOperatorStandaloneCommandsInJetStreamDomain')
for name in tests:assert re.search(r'^--- PASS: '+name+r' ',log,re.M),name
for name in ('operator_leaf_wire.py','operator_daemon_leaf_wire.py','worker_leaf_wire.py'):
    current=(repo/'scripts'/name).read_bytes()
    assert current==subprocess.check_output(['git','show',state['source']+':scripts/'+name],cwd=repo)
    (here/(name+'.txt.gz')).write_bytes(gzip.compress(current,mtime=0))
sys.path.insert(0,str(repo/'scripts'))
import operator_leaf_wire,operator_daemon_leaf_wire
views=Path(str(root)+'-wire-review');views.mkdir(exist_ok=True)
for label,test,record_name,validator in (
    ('commands','TestOperatorStandaloneCommandsThroughLeaf','standalone.process.json',operator_leaf_wire.validate),
    ('daemons','TestOperatorStandaloneDaemonSignalsThroughLeaf','daemon.process.json',operator_daemon_leaf_wire.validate)):
    selected=[]
    for proof in root.rglob('leaf-proof.json'):
        if json.loads(proof.read_text())['test']==test:selected.append(proof.parent)
    for proof in root.rglob(record_name):
        if proof.parent.name.startswith(test+'_') or proof.parent.name.startswith(test+'-'):selected.append(proof.parent)
    target=views/label;target.mkdir(exist_ok=True)
    for directory in set(selected):
        destination=target/directory.name
        if not destination.exists():shutil.copytree(directory,destination)
        source_files={str(p.relative_to(directory)):hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob('*') if p.is_file()}
        copied_files={str(p.relative_to(destination)):hashlib.sha256(p.read_bytes()).hexdigest() for p in destination.rglob('*') if p.is_file()}
        assert source_files==copied_files
    wire=validator(target)
    (here/(label+'-wire-review.json')).write_text(json.dumps(wire,indent=2)+'\n')
records=[json.loads(p.read_text()) for p in root.rglob('standalone.process.json')]
assert len(records)==68 and len({p['pid'] for p in records})==68
connections=[json.loads(p.read_text()) for p in root.rglob('connection.process.json')]
assert len(connections)==8
assert {(p['command'],p['stage'],p['signal']) for p in connections}=={(c,s,g) for c in ('project','tombstone-loop') for s in ('INFO','PONG') for g in ('terminated','interrupt')}
for proof in connections:
    assert proof['exit_code']==0 and proof['scenario_passed'] is True and 'joined_at' in proof
    if proof['stage']=='INFO':assert proof['client_wire']=='' and proof['server_wire']==''
    if proof['stage']=='PONG':assert 'CONNECT ' in proof['client_wire'] and proof['client_wire'].endswith('PING\r\n')
daemons=[json.loads(p.read_text()) for p in root.rglob('daemon.process.json')];assert len(daemons)==5
binaries=list(root.glob('js-wf-standalone-*/wf'));assert len(binaries)==1
binary=binaries[0];digest=hashlib.sha256(binary.read_bytes()).hexdigest()
info=subprocess.check_output(['/usr/local/bin/go','version','-m',str(binary)],text=True)
assert '-race=true' in info and 'vcs.revision='+state['source'] in info and 'vcs.modified=false' in info
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in info
for proof in records+connections+daemons:
    assert proof['exe_sha256']==digest and proof['build_info']==info
    assert not Path('/proc',str(proof['pid'])).exists()
leaves=[json.loads(p.read_text()) for p in root.rglob('leaf.process.json')]
assert len(leaves)==2 and len({p['pid'] for p in leaves})==2
for proof in leaves:
    exe=Path(proof['exe'])
    assert hashlib.sha256(exe.read_bytes()).hexdigest()==proof['exe_sha256']
    assert subprocess.check_output(['/usr/local/bin/go','version','-m',str(exe)],text=True)==proof['build_info']
    assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in proof['build_info']
    assert not Path('/proc',str(proof['pid'])).exists()
    assert proof['argv'][0]==str(exe) and proof['reaped'] is True and proof['exit_code']==0
files={str(p.relative_to(root)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(root.rglob('*')) if p.is_file()}
manifest=here/'artifacts.json.gz'
if manifest.exists():assert json.loads(gzip.decompress(manifest.read_bytes()))==files
else:manifest.write_bytes(gzip.compress(json.dumps(files,sort_keys=True).encode(),mtime=0))
result=dict(accepted=True,source=state['source'],source_inputs=len(before),artifact_files=len(files),artifact_bytes=sum(p['bytes'] for p in files.values()),service=props,
    standalone_command_processes=len(records),connection_signal_processes=len(connections),leaf_daemon_processes=len(daemons),
    total_compiled_cli_processes=len(records)+len(connections)+len(daemons),leaf_brokers=2,binary_sha256=digest,
    binary_path=str(binary),build_info=info,child_wall_seconds=state['child_wall_seconds'],
    log_sha256=state['log_sha256'],actual_100000_entries_qualified=False,postgres_qualified=False,
    scope='Healthy compiled default/domain/leaf commands, controlled handshake shutdown and leaf daemon startup/running/fatal cuts; no graph-specific compiled fixture or broad fault/release qualification.')
(here/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
