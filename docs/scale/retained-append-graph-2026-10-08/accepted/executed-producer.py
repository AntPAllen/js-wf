import sys,os,json,pathlib,subprocess,hashlib,shutil,time,importlib.util
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-retained-append-graph-20261008');assert not root.exists();sys.path.insert(0,str(repo/'scripts'));sys.path.insert(0,'/tmp')
import fixture_archive,live_process_admission
from storage_review_common import closure
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();before=shared.source_inventory(revision)
root.mkdir();save=lambda n,v:(root/n).write_text(json.dumps(v,indent=2)+'\n')
save('source-before.json',before);shutil.copy2(__file__,root/'executed-producer.py');shutil.copy2('/tmp/storage_review_common.py',root/'executed-closure.py')
for name in before['files']:
 target=root/'selected-source'/name
 if target.suffix=='.go':target=target.with_suffix('.go.txt')
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,target)
env=dict(os.environ,GOMAXPROCS='1',GOMEMLIMIT='512MiB');results=[]
for label,race in [('normal',False),('race',True)]:
 binary=root/(label+'.test');build=['go','test','-p=1','-buildvcs=true']+(['-race'] if race else [])+['-c','-o',str(binary),'./internal/retainedgraph']
 with (root/(label+'-build.log')).open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert ('-race=true' in info)==race and 'vcs.revision='+revision in info and 'vcs.modified=false' in info
 fingerprint=shared.sha(binary);save(label+'-binary.json',dict(sha256=fingerprint,build_info=info))
 inventory=subprocess.check_output([str(binary),'-test.list=^TestAppendGraph'],cwd=repo,env=env,text=True);(root/(label+'-inventory.txt')).write_text(inventory)
 command=[str(binary),'-test.run=^TestAppendGraph','-test.v=test2json','-test.count=1','-test.timeout=5m'];save(label+'-commands.json',dict(build=build,run=command));started=time.monotonic()
 with (root/(label+'-actual.log')).open('wb') as log,(root/(label+'-events.jsonl')).open('wb') as events,(root/(label+'-converter.log')).open('wb') as converter_errors:
  converter=subprocess.Popen(['go','tool','test2json','-t','-p','js-wf/internal/retainedgraph'],cwd=repo,env=env,stdin=subprocess.PIPE,stdout=events,stderr=converter_errors)
  child=subprocess.Popen(command,cwd=repo,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  save(label+'-actual-sdk.json',live_process_admission.admit(child,command,{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT']},repo,binary,fingerprint));print('ADMITTED',label,child.pid,flush=True)
  while block:=child.stdout.read(65536):log.write(block);converter.stdin.write(block)
  code=child.wait();converter.stdin.close();converted=converter.wait()
 result=dict(label=label,source=revision,exit_code=code,converter_exit_code=converted,elapsed_seconds=time.monotonic()-started);save(label+'-execution.json',result);results.append(result);print('TERMINAL',result,flush=True)
 assert shared.sha(binary)==fingerprint
 if code!=0 or converted!=0:break
save('execution.json',dict(source=revision,producer_pid=os.getpid(),results=results))
after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('closure.json',closure(root))
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('COMPLETE',results,flush=True);assert len(results)==2 and all(r['exit_code']==r['converter_exit_code']==0 for r in results)
