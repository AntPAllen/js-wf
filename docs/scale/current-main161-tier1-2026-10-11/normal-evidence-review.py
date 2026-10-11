import gzip,hashlib,json
from pathlib import Path
base=Path(__file__).resolve().parent
files=json.loads((base/'normal-terminal-files.json').read_text())
assert len({r['retained'] for r in files})==len(files)
for r in files:
 stored=(base/r['retained']).read_bytes()
 assert len(stored)==r['retained_bytes'] and hashlib.sha256(stored).hexdigest()==r['retained_sha256']
 data=gzip.decompress(stored) if r['encoding']=='gzip' else stored
 assert len(data)==r['bytes'] and hashlib.sha256(data).hexdigest()==r['sha256']
 assert data==Path(r['original']).read_bytes()
result=json.loads((base/'normal-review.json').read_text())
assert result['accepted'] and result['seed_families']==161 and result['saved_traces']==894
assert result['completed_seed_bodies']==161000 and result['actual_package_processes']==8
assert result['binary_sha256']==hashlib.sha256(Path('/home/exedev/js-wf-main161-tier1-20261011/normal1000/sim.test').read_bytes()).hexdigest()
print(json.dumps({'closed_nonbinary_files_verified':len(files),'retained_bytes':sum(r['retained_bytes'] for r in files),'retained_local_binary_sha256':result['binary_sha256']}))
