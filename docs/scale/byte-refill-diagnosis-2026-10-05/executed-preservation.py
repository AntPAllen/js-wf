from pathlib import Path
import subprocess,shutil,json
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/byte-refill-diagnosis-2026-10-05'
rows=[('failed-setup','js-wf-audit-iterator-controls-20261005'),('failed-small-window','js-wf-audit-iterator-controls-corrected-20261005'),('failed-unwrapped-refill','js-wf-audit-iterator-controls-production-window-20261005'),('baseline','js-wf-byte-refill-baseline-20261005'),('first-continuous','js-wf-byte-refill-corrected-20261005'),('iterator-controls','js-wf-audit-iterator-controls-continuous-20261005'),('sdk-source','js-wf-byte-refill-sdk-source-20261005')]
for label,name in rows:
 root=Path('/tmp')/name;dest=out/label
 subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(dest)],check=True,stdout=(Path('/tmp')/f'js-wf-byte-refill-preserve-{label}-20261005.log').open('w'))
 for filename in ['execution.json','normal-test.log','race-test.log','normal-faults.log','source-verification.json']:
  if (root/filename).exists():(dest/filename).write_bytes((root/filename).read_bytes())
 print('PRESERVED',label,flush=True)
(out/'executed-preservation.py').write_bytes(Path(__file__).read_bytes())
