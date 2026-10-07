import sys,subprocess,json,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
label=sys.argv[1]
proof=repo/('docs/scale/tmp-storage-review-2026-10-07/eighth-closed-roots/js-wf-scheduler-complete-originals-20261003' if label=='stable' else 'docs/scale/scheduler-upstream-rc1-2026-10-06')
receipt=json.loads((proof/'s3-readback.json').read_text());meta=json.loads((proof/'archive-verification.json').read_text());inv=json.loads((proof/'fixture-inventory.json').read_text())
archive=Path('/tmp')/('scheduler-rc2-compat-'+label+'-20261007.tar.gz');destination=archive.with_suffix('').with_suffix('')
assert not archive.exists() and not destination.exists()
with archive.open('xb') as f:
 subprocess.run(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--max-time','300',receipt['archive']['url']],input=s3.credentials(),stdout=f,check=True)
restored=fixture_archive.restore(archive,{'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']},inv,destination)
Path('/tmp/scheduler-rc2-compat-'+label+'-restore-20261007.json').write_text(json.dumps({'canonical_metadata':str(proof/'archive-verification.json'),'receipt':str(proof/'s3-readback.json'),'archive':str(archive),'destination':str(destination),'restoration':restored,'qualification':'Read-only source/proof compatibility; no test/broker/store process started.'},indent=2)+'\n')
print(label,restored)
