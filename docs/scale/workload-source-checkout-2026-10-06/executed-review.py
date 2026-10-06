from pathlib import Path
import datetime,hashlib,json,subprocess,shutil,os
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-workload-checkout-46171e6');out=repo/'docs/scale/workload-source-checkout-2026-10-06'
report=json.loads((out/'selected-inputs.json').read_text())
def git(*args):return subprocess.check_output(['git',*args],cwd=root)
assert not git('status','--porcelain')
assert git('rev-parse','HEAD').decode().strip()==report['revision']
selected=report['inputs'];expected={};tracked_bytes=0;omitted_bytes=0
for row in git('ls-tree','-rlz','HEAD').split(b'\0'):
 if not row:continue
 meta,name=row.split(b'\t',1);mode,typ,oid,size=meta.decode().split();name=name.decode()
 assert typ=='blob' and mode in ('100644','100755')
 tracked_bytes+=int(size)
 if name not in selected:
  assert name.startswith('docs/') and not name.endswith(('.go','.py','.yml'))
  assert not (root/name).exists(),name
  omitted_bytes+=int(size);continue
 data=(root/name).read_bytes()
 assert len(data)==int(size)==selected[name]['bytes']
 assert hashlib.sha256(data).hexdigest()==selected[name]['sha256']
 assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid,name
 assert (bool((root/name).stat().st_mode&0o111))==(mode=='100755'),name
 expected[name]=oid
assert set(expected)==set(selected)
actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and '.git' not in p.relative_to(root).parts}
assert actual==set(expected),(actual-set(expected),set(expected)-actual)
command=['go','test','-p=1','./...','-run','^$','-count=1']
with (out/'compile.log').open('x') as log:
 p=subprocess.run(command,cwd=root,env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB'),stdout=log,stderr=subprocess.STDOUT)
assert p.returncode==0,p.returncode
assert not git('status','--porcelain') and git('rev-parse','HEAD').decode().strip()==report['revision']
shutil.copyfile(__file__,out/'executed-review.py')
r=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),revision=report['revision'],selected_files_git_blob_size_sha256_mode_equal=len(expected),all_materialized_files_equal_selection=True,git_clean_before_after=True,tracked_bytes=tracked_bytes,omitted_docs_bytes=omitted_bytes,materialized_bytes=report['materialized_bytes'],compile_command=command,compile_exit=p.returncode,compile_profile=dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB'),compile_log_sha256=hashlib.sha256((out/'compile.log').read_bytes()).hexdigest(),scope='Independent full repository sparse source selection and all-package compilation/skip check only; no native or hosted full-matrix qualification. Runtime/fixture/verifier inputs and source revision preserved; canonical Git proof archive tree omitted from workload checkout.')
(out/'review.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r),flush=True)
