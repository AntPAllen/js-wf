#!/usr/bin/env python3
from pathlib import Path
import subprocess,hashlib,json,shutil,os,sys,importlib.util,time
import argparse
repo=Path(__file__).resolve().parents[1]
sys.dont_write_bytecode=True
parser=argparse.ArgumentParser(description='Compare original, strict and contiguous catchup guards using source-bound race binaries.')
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
for label,name in [('fixture','scripts/fixtures/nats-raft-obsolete-catchup_test.go.txt'),('patcher','scripts/raft-obsolete-catchup-candidate.py')]:
 data=subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo);assert data==(repo/name).read_bytes();(root/(label+('.go' if label=='fixture' else '.py'))).write_bytes(data)
module=Path(subprocess.check_output(['go','list','-m','-f','{{.Dir}}','github.com/nats-io/nats-server/v2'],cwd=repo,text=True).strip());copied=root/'nats-source';shutil.copytree(module,copied)
module_before=fixture_archive.inventory(copied);save('nats-source-before.json',module_before)
for variant in ['strict','contiguous']:
 subprocess.run(['python3',str(repo/'scripts/raft-obsolete-catchup-candidate.py'),'--original',str(copied/'server/raft.go'),'--output',str(root/(variant+'-raft.go')),'--variant',variant],cwd=repo,check=True)
(root/'reference.mod').write_text((repo/'go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(copied)+'\n');shutil.copyfile(repo/'go.sum',root/'reference.sum')
virtual=copied/'server/diagnostic_obsolete_catchup_test.go'
overlays={}
for label in ['upstream','strict','contiguous']:
 replacements={str(virtual):str(root/'fixture.go')}
 if label!='upstream':replacements[str(copied/'server/raft.go')]=str(root/(label+'-raft.go'))
 save(label+'-overlay.json',dict(Replace=replacements));overlays[label]=replacements
base=['-modfile='+str(root/'reference.mod')]
temporary=root/'test-tmp';temporary.mkdir()
env={k:v for k,v in os.environ.items() if not k.startswith(('WF_','MATRIX_','TIER3_MATRIX_'))}
env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',TMPDIR=str(temporary))
save('environment.json',{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','TMPDIR']})
external={};generated=[]
for label in ['upstream','strict','contiguous']:
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
regression='^TestDiagnosticRaftObsoleteCatchup'
controls='^TestNRG(IgnoreEntryAfterCanceledCatchup|ParallelCatchupRollback|RejectAppendEntryDuringCatchupFromPreviousLeader|DelayedMessagesAfterCatchupDontCountTowardQuorum|CatchupAcksProgressWhenGivenProgressInbox|CatchupSendsProgressInboxAndRefillsWindow|CatchupCanTruncateMultipleEntriesWithoutQuorum|CatchupDoesNotTruncateCommittedEntriesDuringRedelivery|TermDoesntRollBackToPtermOnCatchup|CatchupFromNewLeaderWithIncorrectPterm|NewEntriesFromOldLeaderResetsWALDuringCatchup)$'
results={}
for label in ['upstream','strict','contiguous']:
 binary=root/(label+'.test');build=['go','test','-c','-race','-p=1','-mod=readonly',*base,'-overlay='+str(root/(label+'-overlay.json')),'-o',str(binary),'github.com/nats-io/nats-server/v2/server']
 with (root/(label+'-build.log')).open('w') as f:subprocess.run(build,cwd=repo,env=env,check=True,stdout=f,stderr=subprocess.STDOUT)
 info=dict(build=build,executable_sha256=shared.sha(binary),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True));save(label+'-binary.json',info)
 for scope,pattern in [('regression',regression),('controls',controls)]:
  command=[str(binary),'-test.run='+pattern,'-test.v','-test.count=1','-test.timeout=3m']
  with (root/(label+'-'+scope+'.log')).open('w') as f:
   child=subprocess.Popen(command,cwd=copied/'server',env=env,stdout=f,stderr=subprocess.STDOUT);proc=Path('/proc',str(child.pid));actual=dict(pid=child.pid,stat=(proc/'stat').read_text(),argv=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],actual_executable_sha256=shared.sha(proc/'exe'),cwd=os.readlink(proc/'cwd'));code=child.wait()
  save(label+'-'+scope+'-execution.json',dict(command=command,actual=actual,exit_code=code));results[label+'-'+scope]=code
  print(label,scope,code,flush=True)
expected={'upstream-regression':1,'upstream-controls':0,'strict-regression':1,'strict-controls':1,'contiguous-regression':0,'contiguous-controls':0}
expected_outcomes_matched=results==expected
for path,row in external.items():assert shared.sha(path)==shared.sha(root/row['captured'])==row['sha256']
assert fixture_archive.inventory(copied)==module_before;save('nats-source-after.json',module_before);save('dependencies-after.json',external)
after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('closure.json',shared.closure(root));save('result.json',dict(source=revision,results=results,expected=expected,expected_outcomes_matched=expected_outcomes_matched,scope='Twelve direct NATS state-transition cases plus eleven unchanged upstream catchup controls under race. Strict candidate negative control preserved; experimental contiguous overlay only; no installed production change, native matrix or workflow Tier1 qualification.'))
shutil.copyfile(__file__,root/'executed-producer.py');proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1);print(json.dumps(proof),flush=True)

raise SystemExit(0 if expected_outcomes_matched else 1)
