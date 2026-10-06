from pathlib import Path
import subprocess,json,hashlib,shutil,os,datetime,time,sys,signal
import argparse,re
parser=argparse.ArgumentParser(description='Audit fresh copies of a closed retained Tier2 case, including optional copied block-image mount. Native row and full-matrix verdicts remain separate.')
parser.add_argument('--donor',type=Path,required=True)
parser.add_argument('--root',type=Path,required=True)
parser.add_argument('--mount-copied-block-image',action='store_true')
parser.add_argument('--reviewed-donor',action='store_true',help='Use separately reviewed native/durable evidence after a producer generated-cache after-check failure; original failure stays recorded.')
parser.add_argument('--canonical-proof',type=Path,help='Repository relative pushed native proof directory; verify archive/original bytes and visible task descriptors before copying. Applies to successful ordinary non-block donors.')
options=parser.parse_args()
repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
donor=options.donor.resolve();r=options.root.resolve()
if not options.donor.is_absolute() or not options.root.is_absolute() or r.is_relative_to(repo) or r.is_relative_to(donor) or donor.is_relative_to(r):
 parser.error('require absolute donor/root, fresh root outside donor and checkout')
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
from matrix_process_observer import observe_servers
canonical_verification=None
if options.canonical_proof:
 if options.reviewed_donor or options.mount_copied_block_image:parser.error('--canonical-proof supports ordinary successful non-block donors only')
 import importlib.util
 verifier=Path(__file__).with_name('verify-tier2-closed-originals.py')
 spec=importlib.util.spec_from_file_location('copied_original_verifier',verifier);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 verifier_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
 assert verifier.read_bytes()==subprocess.check_output(['git','show',verifier_head+':scripts/verify-tier2-closed-originals.py'],cwd=repo)
 canonical_verification=module.verify(donor,options.canonical_proof)

def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def hashes(root):
 assert all(p.name=='node-2' for p in root.rglob('*') if p.is_symlink())
 return {str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}
e=json.loads((donor/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path(f"/proc/{e['pid']}").exists()
if options.reviewed_donor:
 review=json.loads((donor/'review-acceptance.json').read_text())
 assert review['native_exit_code']==0 and review['review_duration_checker_exit']==0 and review['original_producer_failed_after_cache_cleanup']
else:assert json.loads((donor/'acceptance.json').read_text())['exit_code']==0
assert re.fullmatch(r'TestMixedMatrix[A-Za-z0-9]+',e['test'])
source=donor/'originals'/e['test'];block=e['row']=='blockdisk'
if block and not options.mount_copied_block_image:parser.error('block donor requires explicit --mount-copied-block-image; only copied media is mounted')
if not block and options.mount_copied_block_image:parser.error('copied-image mount applies only to blockdisk donors')
retained=re.findall(r'MATRIX_RETAINED row=\w+ report=\{Invocations:(\d+) Journals:(\d+) Entries:(\d+) Terminal:(\d+)\}',(donor/'native.log').read_text());assert len(retained)==1
count,journals,entries,terminals=retained[0];assert count==journals==terminals and int(count)>0 and int(entries)>=int(count)
assert not any(line.split()[4].startswith(str(source)+'/') for line in Path('/proc/self/mountinfo').read_text().splitlines())
for link in source.rglob('*'):
 if link.is_symlink():assert block and link.name=='node-2' and link.resolve().is_relative_to(source)
for process in Path('/proc').glob('[0-9]*'):
 try:
  for fd in (process/'fd').iterdir():
   try: target=fd.resolve();assert not target.is_relative_to(source)
   except (FileNotFoundError,PermissionError):pass
 except (FileNotFoundError,PermissionError):pass
r.mkdir(mode=0o700);shutil.copy2(repo/'scripts/matrix_process_observer.py',r/'executed-observer.py');(r/'originals').mkdir();original=hashes(source);assert original
if canonical_verification:
 assert sha(donor/'archive-manifest.json')==canonical_verification['archive_manifest_sha256']
 manifest=json.loads((donor/'archive-manifest.json').read_text());prefix=str(source.relative_to(donor))+'/'
 assert original=={name[len(prefix):]:value['sha256'] for name,value in manifest.items() if name.startswith(prefix)}
 (r/'precopy-verification.json').write_text(json.dumps(canonical_verification,indent=2)+'\n');shutil.copy2(verifier,r/'executed-precopy-verifier.py')
shutil.copytree(source,r/'originals'/'cluster',symlinks=True);assert hashes(r/'originals'/'cluster')==original
(r/'copy-before.json').write_text(json.dumps({'original_root':str(source),'original_source':e['source'],'original_native_status':e['status'],'original_sdk_sha256':e['sha256'],'files':original,'all_initial_copy_bytes_match':True},indent=2)+'\n')
shutil.copy2(__file__,r/'executed-producer.py')
helper_source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();template=subprocess.check_output(['git','show',helper_source+':scripts/matrix-retained-review.go.txt'],cwd=repo);assert template==(repo/'scripts/matrix-retained-review.go.txt').read_bytes();(r/'helper.go').write_bytes(template)
assert Path(__file__).read_bytes()==subprocess.check_output(['git','show',helper_source+':scripts/run-tier2-copied-audit.py'],cwd=repo)
assert (r/'executed-observer.py').read_bytes()==subprocess.check_output(['git','show',helper_source+':scripts/matrix_process_observer.py'],cwd=repo)
bound=json.loads((donor/'source-before.json').read_text());external=json.loads((donor/'external-source-before.json').read_text());captured=json.loads((donor/'external-captured-paths.json').read_text())
assert bound==json.loads((donor/'source-after.json').read_text())
if options.reviewed_donor:
 generated=json.loads((donor/'review-generated-inputs.json').read_text());assert generated
 for name,data in generated.items():
  assert data['source_cache_path_missing_after_cleanup'] and external[name]==data['sha256']
  assert sha(donor/data['captured'])==data['sha256'] and b"// Code generated by 'go test'. DO NOT EDIT." in (donor/data['captured']).read_bytes()[:120]
 external={n:h for n,h in external.items() if n not in generated}
 assert external==json.loads((donor/'review-durable-external-after.json').read_text())
else:assert external==json.loads((donor/'external-source-after.json').read_text())
external_by_capture={relative:external[path] for path,relative in captured.items() if path in external}
modules=Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip());goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip())
deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(r/'helper.go')],cwd=repo,text=True);(r/'dependencies.txt').write_text(deps)
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
   rel=Path('modules')/path.relative_to(modules) if path.is_relative_to(modules) else Path('toolchain')/path.relative_to(goroot) if path.is_relative_to(goroot) else Path('other')/str(path).lstrip('/')
   captured_path=str(Path('selected-external-source')/rel)
   assert external_by_capture[captured_path]==digest==sha(donor/captured_path)
   dest=r/'selected-external-source'/str(path).lstrip('/')
  dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest);inputs[str(path)]={'sha256':digest,'captured':str(dest.relative_to(r))}
for name in ['go.mod','go.sum']:
 dest=r/'selected-source'/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/name,dest);assert sha(dest)==bound['files'][name]
(r/'selected-inputs.json').write_text(json.dumps({'production_source':e['source'],'helper_git_source':helper_source,'helper_git_sha256':hashlib.sha256(template).hexdigest(),'files':inputs,'module_files':{n:bound['files'][n] for n in ['go.mod','go.sum']}},indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')};env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB');env['GOCACHE']=subprocess.check_output(['go','env','GOCACHE'],text=True).strip()
build=['go','build','-p=1','-buildvcs=true','-o',str(r/'review-sdk'),str(r/'helper.go')]
with (r/'build.log').open('w') as f:subprocess.run(build,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
binary={'sha256':sha(r/'review-sdk'),'build_info':subprocess.check_output(['go','version','-m',str(r/'review-sdk')],text=True)};(r/'binary.json').write_text(json.dumps(binary,indent=2)+'\n')
histories=list(donor.glob('matrix-*-history.jsonl'));assert len(histories)==1;shutil.copy2(histories[0],r/'history.jsonl')
args=[str(r/'review-sdk'),str(r/'originals'/'cluster'),count,entries,str(r/'history.jsonl'),str(r/'retained-review.json')];(r/'commands.json').write_text(json.dumps({'build':build,'run':args,'environment':{k:env[k] for k in ['GOCACHE','GOMAXPROCS','GOMEMLIMIT']}},indent=2)+'\n')
mount=None
if block:
 images=list((r/'originals'/'cluster').glob('wf-block-*/backing.img'));assert len(images)==1
 media=json.loads((donor/'block-media.json').read_text());assert len(media['images'])==1
 assert sha(images[0])==media['images'][0]['sha256'] and images[0].stat().st_size==512*1024*1024
 mount=r/'originals'/'cluster'/'node-2';assert mount.is_symlink();mount.unlink();mount.mkdir()
 mount_command=['sudo','-n','mount','-o','loop',str(images[0]),str(mount)]
 (r/'copied-mount.json').write_text(json.dumps({'command':mount_command,'original_image_never_mounted':True,'copied_image':str(images[0])},indent=2)+'\n')
 subprocess.run(mount_command,check=True)

child=None
try:
 with (r/'native.log').open('w') as f:
  child=subprocess.Popen(args,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);live=Path(f'/proc/{child.pid}/exe');actual={'pid':child.pid,'actual_sha256':sha(live),'actual_build_info':subprocess.check_output(['go','version','-m',str(live)],text=True),'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_R3_servers_embedded_in_sdk':False};assert actual['actual_sha256']==binary['sha256'];(r/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('COPY_REVIEW_STARTED',child.pid,flush=True)
  seen=set();servers=[]
  while child.poll() is None:
   observe_servers(child.pid,r,seen,servers)
   (r/'observed-servers.json').write_text(json.dumps(servers,indent=2)+'\n')
   try:child.wait(timeout=0.25)
   except subprocess.TimeoutExpired:pass
  code=child.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(r/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
 assert hashes(source)==original
 for path,d in inputs.items():assert sha(Path(path))==sha(r/d['captured'])==d['sha256']
 (r/'original-after.json').write_text(json.dumps({'original_closed_files':hashes(source),'unchanged':True},indent=2)+'\n');(r/'copy-after.json').write_text(json.dumps({'closed_copy_files':hashes(r/'originals'/'cluster')},indent=2)+'\n');shutil.copy2(__file__,r/'executed-producer.py');print('COPY_REVIEW_FINISHED',code,flush=True)
except BaseException:
 (r/'producer-error.txt').write_text(__import__('traceback').format_exc())
 if child is not None and child.poll() is not None and 'actual' in globals():
  actual.update(status='passed' if child.returncode==0 else 'failed',exit_code=child.returncode)
  (r/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
 raise
finally:
 if child is not None and child.poll() is None:
  os.killpg(child.pid,signal.SIGTERM)
  try:child.wait(timeout=5)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
 if mount is not None:subprocess.run(['sudo','-n','umount',str(mount)],check=True)
 (r/'copy-after-detach.json').write_text(json.dumps({'files':hashes(r/'originals'/'cluster'),'copied_mount_detached':True},indent=2)+'\n')

raise SystemExit(code)
