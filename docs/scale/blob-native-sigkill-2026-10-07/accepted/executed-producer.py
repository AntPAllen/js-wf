import sys,os,json,pathlib,subprocess,hashlib,shutil,time,importlib.util
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-native-blob-sigkill-20261007');assert not root.exists();sys.path.insert(0,str(repo/'scripts'))
import fixture_archive,live_process_admission
sys.path.insert(0,'/tmp')
from storage_review_common import closure as complete_closure
shutil_source=pathlib.Path('/tmp/storage_review_common.py')
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();before=shared.source_inventory(revision)
root.mkdir();(root/'stores').mkdir();save=lambda n,v:(root/n).write_text(json.dumps(v,indent=2)+'\n');save('source-before.json',before);shutil.copy2(__file__,root/'executed-producer.py');shutil.copy2(shutil_source,root/'executed-closure.py')
for name in before['files']:
 target=root/'selected-source'/name
 if target.suffix=='.go':target=target.with_suffix('.go.txt')
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,target)
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_BLOB_AUTHORITY_ROOT=str(root/'stores'));binary=root/'blobpublication-race.test'
build=['go','test','-p=1','-buildvcs=true','-race','-c','-o',str(binary),'./internal/blobpublication']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert '-race=true' in info and 'vcs.revision='+revision in info and 'vcs.modified=false' in info and 'github.com/nats-io/nats-server/v2\tv2.15.0' in info
fingerprint=shared.sha(binary);save('binary.json',dict(sha256=fingerprint,build_info=info));command=[str(binary),'-test.v','-test.count=1','-test.timeout=3m'];save('commands.json',dict(build=build,run=command));started=time.monotonic()
with (root/'actual.log').open('w') as log:
 child=subprocess.Popen(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
 save('actual-sdk.json',live_process_admission.admit(child,command,{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','WF_BLOB_AUTHORITY_ROOT']},repo,binary,fingerprint));print('ADMITTED_NATIVE_SIGKILL',child.pid,flush=True);code=child.wait()
save('execution.json',dict(source=revision,exit_code=code,elapsed_seconds=time.monotonic()-started));after=shared.source_inventory(revision);assert after==before;save('source-after.json',after);save('closure.json',complete_closure(root));assert shared.sha(binary)==fingerprint
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('COMPLETE_NATIVE_SIGKILL',code,flush=True);assert code==0
