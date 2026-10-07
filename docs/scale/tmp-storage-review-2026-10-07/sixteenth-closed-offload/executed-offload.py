import sys,json,subprocess
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/tmp-storage-review-2026-10-07/sixteenth-closed-offload'
for item in json.loads((base/'selection.json').read_text())['selected']:
 proof=base/Path(item['root']).name
 subprocess.run([sys.executable,str(repo/'scripts/offload-proof-to-s3.py'),'--archive',item['archive'],'--canonical-metadata',str((proof/'archive-verification.json').relative_to(repo)),'--endpoint','https://nameless-bird-8772.int.exe.xyz','--bucket','nameless-bird-8772','--receipt',str(proof/'s3-readback.json')],cwd=repo,check=True,stdout=subprocess.DEVNULL)
 print('VERIFIED_S3',proof.name,flush=True)
