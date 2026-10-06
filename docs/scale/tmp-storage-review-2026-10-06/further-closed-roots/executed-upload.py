from pathlib import Path
import subprocess,os
repo=Path('/home/exedev/js-wf')
base=repo/'docs/scale/tmp-storage-review-2026-10-06/further-closed-roots'
env=dict(os.environ,AWS_ACCESS_KEY_ID='x',AWS_SECRET_ACCESS_KEY='x')
for out in sorted(p for p in base.iterdir() if p.is_dir()):
 subprocess.run(['python3',str(repo/'scripts/offload-proof-to-s3.py'),'--archive',str(Path('/tmp')/(out.name+'-storage-third-complete-20261006.tar.gz')),'--canonical-metadata',str((out/'archive-verification.json').relative_to(repo)),'--endpoint','https://nameless-bird-8772.int.exe.xyz','--bucket','nameless-bird-8772','--receipt',str(out/'s3-readback.json')],check=True,env=env)
 print('REMOTE_VERIFIED',out.name,flush=True)
