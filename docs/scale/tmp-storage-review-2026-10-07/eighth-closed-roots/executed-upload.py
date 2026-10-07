from pathlib import Path
import subprocess,os
from concurrent.futures import ThreadPoolExecutor
repo=Path('/home/exedev/js-wf')
base=repo/'docs/scale/tmp-storage-review-2026-10-07/eighth-closed-roots'
env=dict(os.environ,AWS_ACCESS_KEY_ID='x',AWS_SECRET_ACCESS_KEY='x')
def upload(out):
 if (out/'s3-readback.json').exists():return
 subprocess.run(['python3',str(repo/'scripts/offload-proof-to-s3.py'),'--archive',str(Path('/tmp')/(out.name+'-storage-eighth-complete-20261007.tar.gz')),'--canonical-metadata',str((out/'archive-verification.json').relative_to(repo)),'--endpoint','https://nameless-bird-8772.int.exe.xyz','--bucket','nameless-bird-8772','--receipt',str(out/'s3-readback.json')],check=True,env=env)
 print('REMOTE_VERIFIED',out.name,flush=True)

with ThreadPoolExecutor(max_workers=4) as pool:
 for result in pool.map(upload, sorted(p for p in base.iterdir() if p.is_dir() and (p/'archive-verification.json').exists())):pass
