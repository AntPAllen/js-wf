import sys,os,json,pathlib,subprocess,hashlib,shutil,time,importlib.util
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-native-object-permissions-20261008');assert not root.exists();sys.path.insert(0,str(repo/'scripts'))
import fixture_archive,live_process_admission
sys.path.insert(0,'/tmp')
from storage_review_common import closure
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();before=shared.source_inventory(revision)
root.mkdir();(root/'blob-stores').mkdir()
save=lambda n,v:(root/n).write_text(json.dumps(v,indent=2)+'\n')
save('source-before.json',before);shutil.copy2(__file__,root/'executed-producer.py');shutil.copy2('/tmp/storage_review_common.py',root/'executed-closure.py')
for name in before['files']:
 target=root/'selected-source'/name
 if target.suffix=='.go':target=target.with_suffix('.go.txt')
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,target)
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_BLOB_AUTHORITY_ROOT=str(root/'blob-stores'))
results=[]
for label,package,selector in [('blob','./internal/blobpublication','^(TestAuthoritySubjectAccessRejectsInvalidNamespaces|TestNativeObjectRuntimePermissions)$')]:
 binary=root/(label+'-race.test');build=['go','test','-p=1','-buildvcs=true','-race','-c','-o',str(binary),package]
 with (root/(label+'-build.log')).open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
 assert '-race=true' in info and 'vcs.revision='+revision in info and 'vcs.modified=false' in info and 'github.com/nats-io/nats-server/v2\tv2.15.0' in info
 fingerprint=shared.sha(binary);save(label+'-binary.json',dict(sha256=fingerprint,build_info=info))
 command=[str(binary),'-test.run='+selector,'-test.v','-test.count=1','-test.timeout=3m'];save(label+'-commands.json',dict(build=build,run=command));started=time.monotonic()
 with (root/(label+'-actual.log')).open('w') as log:
  child=subprocess.Popen(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
  save(label+'-actual-sdk.json',live_process_admission.admit(child,command,{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','WF_BLOB_AUTHORITY_ROOT']},repo,binary,fingerprint));print('ADMITTED',label,child.pid,flush=True);code=child.wait()
 result=dict(label=label,source=revision,exit_code=code,elapsed_seconds=time.monotonic()-started);save(label+'-execution.json',result);results.append(result)
 assert shared.sha(binary)==fingerprint
 if code!=0:break
save('execution.json',dict(source=revision,results=results))
after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('closure.json',closure(root))
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('COMPLETE',results,flush=True);assert len(results)==1 and all(r['exit_code']==0 for r in results)
