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
assert state['command']==['/usr/local/bin/go','test','-race','-p=1','./cmd/wf','./visibility','-run=^(TestOperatorStandaloneCanonicalGraphPostgresCommands|TestGraphPostgresVisibilityCanonicalRowsAndUncertainty|TestPostgresNamespaceIsolation)$','-count=1','-v','-timeout=20m']
assert state['configuration']=={'WF_OPERATOR_STANDALONE':'1','WF_OPERATOR_TEST_ROOT':str(root),'GOWORK':'off','GOFLAGS':'','GOMAXPROCS':'2','GOMEMLIMIT':'1GiB','WF_TEST_POSTGRES_DSN':'postgres://postgres@127.0.0.1:15438/wf_canonical_cli?sslmode=disable'}
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
assert not any(x in log for x in ('--- FAIL:','WARNING: DATA RACE','panic:'))
assert re.search(r'^ok\s+js-wf/visibility\s+',log,re.M)
assert not re.search(r'^--- SKIP:', log, re.M)
for name in ('TestOperatorStandaloneCanonicalGraphPostgresCommands','TestGraphPostgresVisibilityCanonicalRowsAndUncertainty','TestPostgresNamespaceIsolation'):
    assert re.search(r'^--- PASS: '+name+r' ',log,re.M),name
launch=json.loads((here/'launch.json').read_text())
assert launch['invocation']==state['invocation'] and launch['supervisor_pid']==state['supervisor_pid'] and launch['child_pid']==state['child_pid']
assert launch['source']==state['source'] and launch['command']==state['command']
assert hashlib.sha256((here/'supervisor.py').read_bytes()).hexdigest()==launch['supervisor_sha256']
for name in ('canonical_cli_wire.py','worker_leaf_wire.py'):
    current=(repo/'scripts'/name).read_bytes()
    assert current==subprocess.check_output(['git','show',state['source']+':scripts/'+name],cwd=repo)
    (here/(name+'.txt.gz')).write_bytes(gzip.compress(current,mtime=0))
sys.path.insert(0,str(repo/'scripts'))
import canonical_cli_wire
records=[json.loads(p.read_text()) for p in root.rglob('standalone.process.json')]
assert len(records)==48 and len({p['pid'] for p in records})==48
canonical=[]
for path in root.rglob('standalone.process.json'):
    if path.parent.name.startswith('TestOperatorStandaloneCanonicalGraphPostgresCommands_'):
        proof=json.loads(path.read_text());expect=json.loads((path.parent/'wire-expectation.json').read_text())
        assert proof['domain'] in ('','WFGRAPHOPS') and proof['argv'][1:]==expect['args']
        offline=expect['offline'];
        if '-postgres-dsn' in proof['argv']:
            assert not offline and '-postgres-dsn' in proof['argv'] and proof['argv'][proof['argv'].index('-postgres-dsn')+1]==state['configuration']['WF_TEST_POSTGRES_DSN']
            assert '-graph-view-namespace' in proof['argv'] and 'Postgres' in proof['argv'][proof['argv'].index('-graph-view-namespace')+1]
        assert offline==('-replay-bundle' in proof['argv'])
        if not offline:
            assert '-graph-authority-stream' in proof['argv']
            if proof['domain']:assert proof['argv'][proof['argv'].index('-domain')+1]==proof['domain']
            else:assert '-domain' not in proof['argv']
        cleanup=None
        if proof['argv'][-3:]==['purge','graph-operator','success']:
            cleanup='wf.jrn.graph-operator.success'
        wire=canonical_cli_wire.validate(path.parent,proof['domain'],offline,cleanup)
        canonical.append(dict(pid=proof['pid'],domain=proof['domain'],exit_code=proof['exit_code'],postgres_view=('-postgres-dsn' in proof['argv']),wire=wire))
assert len(canonical)==48
for domain,case in (('','R1'),('WFGRAPHOPS','R3Domain')):
    selected=[p for p in canonical if p['domain']==domain]
    assert len(selected)==24 and sum(p['wire']['offline'] for p in selected)==1
    assert sum(p['postgres_view'] for p in selected)==2
    assert re.search(r'--- PASS: TestOperatorStandaloneCanonicalGraphPostgresCommands/'+case+r' ',log)
projects=[]
for path in root.rglob('project.process.json'):
    proof=json.loads(path.read_text())
    assert proof['exit_code']==0 and proof['reaped'] is True
    assert proof['domain'] in ('','WFGRAPHOPS')
    assert '-postgres-dsn' in proof['argv'] and '-graph-view-namespace' in proof['argv']
    assert proof['argv'][proof['argv'].index('-postgres-dsn')+1]==state['configuration']['WF_TEST_POSTGRES_DSN']
    assert 'Postgres' in proof['argv'][proof['argv'].index('-graph-view-namespace')+1]
    assert proof['argv'][-1]=='project' and '-graph-authority-stream' in proof['argv']
    assert not Path('/proc',str(proof['pid'])).exists()
    projects.append(dict(proof=proof,wire=canonical_cli_wire.validate(path.parent,proof['domain'])))
assert len(projects)==2 and {p['proof']['domain'] for p in projects}=={'','WFGRAPHOPS'}
(here/'canonical-wire-review.json').write_text(json.dumps(dict(commands=canonical,projectors=projects),indent=2)+'\n')
binaries=list(root.glob('js-wf-standalone-*/wf'));assert len(binaries)==1
binary=binaries[0];digest=hashlib.sha256(binary.read_bytes()).hexdigest()
info=subprocess.check_output(['/usr/local/bin/go','version','-m',str(binary)],text=True)
assert '-race=true' in info and 'vcs.revision='+state['source'] in info and 'vcs.modified=false' in info
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in info
for proof in records+[p['proof'] for p in projects]:
    assert proof['exe_sha256']==digest and proof['build_info']==info
    assert not Path('/proc',str(proof['pid'])).exists()
files={str(p.relative_to(root)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(root.rglob('*')) if p.is_file()}
manifest=here/'artifacts.json.gz'
if manifest.exists():assert json.loads(gzip.decompress(manifest.read_bytes()))==files
else:manifest.write_bytes(gzip.compress(json.dumps(files,sort_keys=True).encode(),mtime=0))
result=dict(accepted=True,source=state['source'],source_inputs=len(before),artifact_files=len(files),artifact_bytes=sum(p['bytes'] for p in files.values()),service=props,
    compiled_canonical_commands=48,compiled_canonical_projectors=2,binary_sha256=digest,binary_path=str(binary),build_info=info,
    child_wall_seconds=state['child_wall_seconds'],log_sha256=state['log_sha256'],postgres_qualified=True,
    scope='Focused compiled canonical PostgreSQL R1/R3 commands and projectors plus nine visibility uncertainty/terminal cases and namespace isolation. No full-package, scale, crash-rebuild, storage/fault or release acceptance.')
(here/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
