import sys
sys.dont_write_bytecode=True
from pathlib import Path
import subprocess,json,importlib.util,hashlib,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
proof=repo/'docs/scale/lease-partition-component-2026-10-06/contiguous-component'
root=Path('/tmp/js-wf-candidate-partition200-input-20261007');root.mkdir()
meta=json.loads((proof/'archive-verification.json').read_text());inventory=json.loads((proof/'fixture-inventory.json').read_text());receipt=json.loads((proof/'s3-readback.json').read_text())
assert hashlib.sha256((proof/'fixture-inventory.json').read_bytes()).hexdigest()==meta['inventory_sha256']
expected={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']};assert receipt['archive']['full_readback']==expected
archive=root/'component.tar.gz'
with archive.open('xb') as output:
 get=subprocess.run(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],input=s3.credentials(),stdout=output,stderr=subprocess.PIPE)
 assert get.returncode==0,get.stderr
with archive.open('rb') as stream:declared,actual=fixture_archive.verify_hashed_stream(stream,expected)
assert declared==inventory
report=fixture_archive.restore(archive,expected,inventory,root/'component')
binary=root/'component/candidate-server'
with binary.open('rb') as stream:assert s3.digest(stream)=={'bytes':inventory['files']['candidate-server']['bytes'],'sha256':'a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68'}
(root/'restore.json').write_text(json.dumps({'parent_proof':str(proof.relative_to(repo)),'parent_archive_url':receipt['archive']['url'],'restored':report,'candidate':str(binary),'executable_sha256':'a56911eef56974eb10eb8b749a46b618100e8662b524f44be37ba06f2de8ad68','scope':'Fresh verified component restore for exact candidate input only; no process or old store opened.'},indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-restore.py');print('CANDIDATE_FULL_RESTORE_VERIFIED',actual,flush=True)
