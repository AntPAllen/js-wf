#!/usr/bin/env python3
from pathlib import Path
import subprocess,hashlib,json,shutil,os,sys,importlib.util,time
import argparse
repo=Path(__file__).resolve().parents[1]
sys.dont_write_bytecode=True
parser=argparse.ArgumentParser(description='Check original and complete-reservation snapshot fixtures with a deterministic late I/O permit.')
parser.add_argument('--root',type=Path,required=True)
args=parser.parse_args();root=args.root.absolute()
assert not root.exists() and not root.is_relative_to(repo)
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert revision==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
before=shared.source_inventory(revision);root.mkdir()
def save(name,data):(root/name).write_text(json.dumps(data,indent=2)+'\n')
save('source-before.json',before)
for name in before['files']:
 target=root/'selected-source'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(repo/name,target)
for label,name in [('reservation','scripts/raft-snapshot-test-reservation.py'),('patcher','scripts/raft-obsolete-catchup-candidate.py')]:
 data=subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo);assert data==(repo/name).read_bytes();(root/(label+'.py')).write_bytes(data)
module=Path(subprocess.check_output(['go','list','-m','-f','{{.Dir}}','github.com/nats-io/nats-server/v2'],cwd=repo,text=True).strip());copied=root/'nats-source';shutil.copytree(module,copied)
module_before=fixture_archive.inventory(copied);save('nats-source-before.json',module_before)
for variant in ['contiguous']:
 subprocess.run(['python3',str(repo/'scripts/raft-obsolete-catchup-candidate.py'),'--original',str(copied/'server/raft.go'),'--output',str(root/(variant+'-raft.go')),'--variant',variant],cwd=repo,check=True)
(root/'reference.mod').write_text((repo/'go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(copied)+'\n');shutil.copyfile(repo/'go.sum',root/'reference.sum')
profiles={'original-late':(False,True),'reserved-late':(True,True),'reserved':(True,False)}
for profile,(reserve_all,late_return) in profiles.items():
 command=['python3',str(repo/'scripts/raft-snapshot-test-reservation.py'),'--original',str(copied/'server/raft_test.go'),'--output',str(root/(profile+'-raft_test.go'))]
 if reserve_all:command.append('--reserve-all')
 if late_return:command.append('--late-return')
 with (root/(profile+'-change.json')).open('w') as f:subprocess.run(command,cwd=repo,check=True,stdout=f)
overlays={}
for production in ['upstream','contiguous']:
 for profile in profiles:
  label=production+'-'+profile
  replacements={str(copied/'server/raft_test.go'):str(root/(profile+'-raft_test.go'))}
  if production!='upstream':replacements[str(copied/'server/raft.go')]=str(root/'contiguous-raft.go')
  save(label+'-overlay.json',dict(Replace=replacements));overlays[label]=replacements
base=['-modfile='+str(root/'reference.mod')]
temporary=root/'test-tmp';temporary.mkdir()
env={k:v for k,v in os.environ.items() if not k.startswith(('WF_','MATRIX_','TIER3_MATRIX_'))}
env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',TMPDIR=str(temporary))
save('environment.json',{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','TMPDIR']})
external={};generated=[]
for label in overlays:
 command=['go','list','-mod=mod',*base,'-overlay='+str(root/(label+'-overlay.json')),'-deps','-test','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}','github.com/nats-io/nats-server/v2/server']
 result=subprocess.run(command,cwd=repo,env=env,check=True,capture_output=True,text=True);(root/(label+'-dependencies.txt')).write_text(result.stdout)
 for line in result.stdout.splitlines():
  directory,*groups=line.split('|')
  for name in ' '.join(groups).split():
   path=Path(directory)/name
   if str(path) in overlays[label]:path=Path(overlays[label][str(path)])
   if not path.exists():
    assert path.name=='_testmain.go',str(path);generated.append(str(path));continue
   digest=shared.sha(path);target=root/'selected-dependencies'/str(path).lstrip('/');target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target);external[str(path)]=dict(sha256=digest,captured=str(target.relative_to(root)))
save('dependencies-before.json',external);save('generated-testmain-limits.json',dict(paths=generated,scope='Go-generated testmain is derived by the pinned compiler; selected repository, fixture, module and dependency source are retained.'))
save('module-inputs.json',dict(mod_sha256=shared.sha(root/'reference.mod'),sum_sha256=shared.sha(root/'reference.sum')))
controls='^TestNRGCheckpointInstallSnapshotAbortDuringWrite$'
results={}
for label in overlays:
 binary=root/(label+'.test');build=['go','test','-c','-race','-p=1','-mod=readonly',*base,'-overlay='+str(root/(label+'-overlay.json')),'-o',str(binary),'github.com/nats-io/nats-server/v2/server']
 with (root/(label+'-build.log')).open('w') as f:subprocess.run(build,cwd=repo,env=env,check=True,stdout=f,stderr=subprocess.STDOUT)
 info=dict(build=build,executable_sha256=shared.sha(binary),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True));save(label+'-binary.json',info)
 for scope,pattern in [('snapshot-controls',controls)]:
  command=[str(binary),'-test.run='+pattern,'-test.v','-test.count=1','-test.timeout=3m']
  with (root/(label+'-'+scope+'.log')).open('w') as f:
   child=subprocess.Popen(command,cwd=copied/'server',env=env,stdout=f,stderr=subprocess.STDOUT);proc=Path('/proc',str(child.pid));deadline=time.monotonic()+5
   while True:
    actual=dict(pid=child.pid,stat=(proc/'stat').read_text(),argv=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],actual_executable_sha256=shared.sha(proc/'exe'),cwd=os.readlink(proc/'cwd'))
    if actual['argv']==command and actual['actual_executable_sha256']==info['executable_sha256']:break
    if child.poll() is not None or time.monotonic()>deadline:raise RuntimeError('native process identity did not stabilize')
    time.sleep(.01)
   live_env={os.fsdecode(v.split(b'=',1)[0]):os.fsdecode(v.split(b'=',1)[1]) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v}
   actual['environment']={k:live_env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','TMPDIR','GOWORK','GOFLAGS']};assert actual['environment']==json.loads((root/'environment.json').read_text())
   actual['build_info']=subprocess.check_output(['go','version','-m',str(proc/'exe')],text=True)
   code=child.wait()
  save(label+'-'+scope+'-execution.json',dict(command=command,actual=actual,exit_code=code));results[label+'-'+scope]=code
  print(label,scope,code,flush=True)
expected={label+'-snapshot-controls':(1 if label.endswith('-original-late') else 0) for label in overlays}
observations={}
import re
for label in overlays:
 log=(root/(label+'-snapshot-controls.log')).read_text()
 names=re.findall(r'^=== RUN   (TestNRG[^ ]+)$',log,re.M)
 expected_names=['TestNRGCheckpointInstallSnapshotAbortDuringWrite','TestNRGCheckpointInstallSnapshotAbortDuringWrite/RemoveOrphan','TestNRGCheckpointInstallSnapshotAbortDuringWrite/KeepAdoptedSnapshot']
 failed=re.findall(r'^\s*--- FAIL: (TestNRG[^ ]+) ',log,re.M)
 passed=re.findall(r'^\s*--- PASS: (TestNRG[^ ]+) ',log,re.M)
 late=re.findall(r'DIAGNOSTIC_LATE_DIOS borrowed=1 waited=true drained=(\d+) capacity=(\d+)',log)
 negative=label.endswith('-original-late')
 late_enabled=label.endswith('-late')
 valid=names==expected_names and 'WARNING: DATA RACE' not in log and '--- SKIP:' not in log
 valid=valid and (set(failed)==set(expected_names) and not passed and log.count('writer returned before dios refill: <nil>')==2 if negative else set(passed)==set(expected_names) and not failed)
 valid=valid and (len(late)==2 and all(0<int(a)<int(b) if negative else int(a)==int(b) for a,b in late) if late_enabled else not late)
 observations[label]=dict(expected_native_failure=negative,started=names,passed=passed,failed=failed,late_permit_observations=late,qualified_control=valid)
save('observations.json',observations)
expected_outcomes_matched=results==expected and all(v['qualified_control'] for v in observations.values())
for path,row in external.items():assert shared.sha(path)==shared.sha(root/row['captured'])==row['sha256']
assert fixture_archive.inventory(copied)==module_before;save('nats-source-after.json',module_before);save('dependencies-after.json',external)
after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('closure.json',shared.closure(root));save('result.json',dict(source=revision,results=results,expected=expected,expected_outcomes_matched=expected_outcomes_matched,scope='One original snapshot test/two original subcases, original late-return negative controls and complete-reservation positive controls with/without the same late-return injection, against upstream and contiguous production under race/count1/3m/2CPU/2GiB. Every original assertion/sleep/deadline retained. Expected native failures preserved. Historical full170 causes remain untraced; no full170/default dependency/matrix/Tier1 qualification.'))
shutil.copyfile(__file__,root/'executed-producer.py');proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1);print(json.dumps(proof),flush=True)

raise SystemExit(0 if expected_outcomes_matched else 1)
