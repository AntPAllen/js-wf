from pathlib import Path
import subprocess,os,concurrent.futures
repo=Path('/home/exedev/js-wf')
base=repo/'docs/scale/tmp-storage-review-2026-10-06/sixth-closed-roots'
env=dict(os.environ,AWS_ACCESS_KEY_ID='x',AWS_SECRET_ACCESS_KEY='x')
def upload(out):
 subprocess.run(['python3',str(repo/'scripts/offload-proof-to-s3.py'),'--archive',str(Path('/tmp')/(out.name+'-storage-sixth-complete-20261006.tar.gz')),'--canonical-metadata',str((out/'archive-verification.json').relative_to(repo)),'--endpoint','https://nameless-bird-8772.int.exe.xyz','--bucket','nameless-bird-8772','--receipt',str(out/'s3-readback.json')],check=True,env=env)
 print('REMOTE_VERIFIED',out.name,flush=True)
ready=sorted(p for p in base.iterdir() if p.is_dir() and (p/'archive-verification.json').exists() and not (p/'s3-readback.json').exists())
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
 futures=[executor.submit(upload,out) for out in ready]
 errors=[]
 for future in futures:
  try:future.result()
  except BaseException as error:errors.append(error)
 if errors:raise errors[0]
