from pathlib import Path
import subprocess,json,hashlib,shutil,os,datetime,time,sys
repo=Path('/home/exedev/js-wf');donor=Path('/tmp/js-wf-fanout-combined-boundaries-20261005');position=sys.argv[1];assert position in ('first','interior','last');r=Path('/tmp/js-wf-fanout-copied-diagnosis-'+position+'-20261005')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def hashes(root):
 assert not any(p.is_symlink() for p in root.rglob('*'))
 return {str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
e=json.loads((donor/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path(f"/proc/{e['pid']}").exists()
source=donor/'originals'/'TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix'/'results'/position/'cluster'
for process in Path('/proc').glob('[0-9]*'):
 try:
  for fd in (process/'fd').iterdir():
   try: target=fd.resolve();assert not target.is_relative_to(source)
   except (FileNotFoundError,PermissionError):pass
 except (FileNotFoundError,PermissionError):pass
r.mkdir();original=hashes(source);assert original
shutil.copytree(source,r/'cluster');assert hashes(r/'cluster')==original
(r/'copy-before.json').write_text(json.dumps({'original_root':str(source),'original_source':e['source'],'original_native_status':e['status'],'original_sdk_sha256':e['sha256'],'files':original,'all_initial_copy_bytes_match':True},indent=2)+'\n')
helper_source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();template=subprocess.check_output(['git','show',helper_source+':scripts/fanout-retained-diagnosis.go.txt'],cwd=repo);assert template==(repo/'scripts/fanout-retained-diagnosis.go.txt').read_bytes();(r/'helper.go').write_bytes(template)
bound=json.loads((donor/'source-before.json').read_text());external=json.loads((donor/'external-source-before.json').read_text());deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(r/'helper.go')],cwd=repo,text=True);(r/'dependencies.txt').write_text(deps)
inputs={}
for line in deps.splitlines():
 directory,*groups=line.split('|')
 for name in ' '.join(groups).split():
  path=(Path(directory)/name).resolve()
  if path==r/'helper.go':continue
  digest=sha(path)
  if path.is_relative_to(repo):
   name=str(path.relative_to(repo));assert bound['files'][name]==digest==sha(donor/'source'/name)==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()
   dest=r/'selected-source'/name
  else:
   assert external[str(path)]==digest;dest=r/'selected-external-source'/str(path).lstrip('/')
  dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest);inputs[str(path)]={'sha256':digest,'captured':str(dest.relative_to(r))}
for name in ['go.mod','go.sum']:
 dest=r/'selected-source'/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,dest);assert sha(dest)==bound['files'][name]
(r/'selected-inputs.json').write_text(json.dumps({'production_source':e['source'],'helper_git_source':helper_source,'helper_git_sha256':hashlib.sha256(template).hexdigest(),'files':inputs,'module_files':{n:bound['files'][n] for n in ['go.mod','go.sum']}},indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')};env.update(GOCACHE='/tmp/js-wf-go-build-cache-20261004',GOMAXPROCS='2',GOMEMLIMIT='2GiB')
build=['go','build','-p=1','-buildvcs=true','-o',str(r/'review-sdk'),str(r/'helper.go')]
with (r/'build.log').open('w') as f:subprocess.run(build,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
binary={'sha256':sha(r/'review-sdk'),'build_info':subprocess.check_output(['go','version','-m',str(r/'review-sdk')],text=True)};(r/'binary.json').write_text(json.dumps(binary,indent=2)+'\n')
args=[str(r/'review-sdk'),str(r/'cluster'),str(r/'retained-review.json')];(r/'commands.json').write_text(json.dumps({'build':build,'run':args,'environment':{k:env[k] for k in ['GOCACHE','GOMAXPROCS','GOMEMLIMIT']}},indent=2)+'\n')
with (r/'native.log').open('w') as f:
 child=subprocess.Popen(args,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT);live=Path(f'/proc/{child.pid}/exe');actual={'pid':child.pid,'actual_sha256':sha(live),'actual_build_info':subprocess.check_output(['go','version','-m',str(live)],text=True),'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_R3_servers_embedded_in_sdk':True};assert actual['actual_sha256']==binary['sha256'];(r/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('COPY_REVIEW_STARTED',child.pid,flush=True)
 code=child.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(r/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
assert hashes(source)==original
for path,d in inputs.items():assert sha(Path(path))==sha(r/d['captured'])==d['sha256']
(r/'original-after.json').write_text(json.dumps({'original_closed_files':hashes(source),'unchanged':True},indent=2)+'\n');(r/'copy-after.json').write_text(json.dumps({'closed_copy_files':hashes(r/'cluster')},indent=2)+'\n');shutil.copy2(__file__,r/'executed-producer.py');print('COPY_REVIEW_FINISHED',code,flush=True);raise SystemExit(code)
