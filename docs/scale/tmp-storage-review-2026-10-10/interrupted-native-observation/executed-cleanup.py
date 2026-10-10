import datetime
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

import boto3

sys.dont_write_bytecode = True
repo = Path('/home/exedev/js-wf')
sys.path.insert(0, str(repo / 'scripts'))
import fixture_archive as fa

base = Path(__file__).resolve().parent
roots = [Path('/tmp/TestNativeGraphContinuationFailedChildPromiseR3Domainarchive=t1123786006'),
         Path('/tmp/go-build194482071')]
stage = Path('/home/exedev/tmp-interrupted-native-observation-stage-20261010')
archive = Path('/home/exedev/tmp-interrupted-native-observation-20261010.tar.gz')

def closure():
    source = repo / 'docs/scale/tmp-storage-review-2026-10-08/remaining-evidence-batch/executed-storage.py'
    code = """import sys,json,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
source,roots=json.load(sys.stdin)
spec=importlib.util.spec_from_file_location('closure',source)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
result=module.closure(list(map(Path,roots)))
for path in Path('/proc').glob('[0-9]*/environ'):
 try: data=path.read_bytes()
 except (FileNotFoundError,ProcessLookupError): continue
 for root in roots:
  if root.encode() in data: result['blocked'].setdefault(root,[]).append(str(path))
print(json.dumps(result))
"""
    result = json.loads(subprocess.check_output(
        ['sudo', '-n', 'python3', '-c', code],
        input=json.dumps([str(source), list(map(str, roots))]), text=True))
    assert not result['blocked'] and not result['permission_limits'], result
    return result

assert not stage.exists() and not archive.exists()
before = closure()
usage_before = subprocess.check_output(['sudo', 'du', '-sx', '/tmp'], text=True).strip()
allocated = sum(p.stat().st_blocks * 512 for root in roots for p in [root, *root.rglob('*')])
stage.mkdir()
for root in roots:
    shutil.copytree(root, stage / root.name)
proof = fa.capture(stage, archive, base, compresslevel=3)
inventory = json.loads((base / 'fixture-inventory.json').read_text())
client = boto3.client('s3', endpoint_url='https://nameless-bird-8772.int.exe.xyz',
                      aws_access_key_id='x', aws_secret_access_key='x')
bucket = 'nameless-bird-8772'
key = 'js-wf/tmp-cleanup/2026-10-10/' + proof['archive_sha256'] + '.tar.gz'
with archive.open('rb') as stream:
    client.put_object(Bucket=bucket, Key=key, Body=stream)
response = client.get_object(Bucket=bucket, Key=key)
try:
    declared, actual = fa.verify_hashed_stream(response['Body'],
        dict(bytes=proof['archive_bytes'], sha256=proof['archive_sha256']))
finally:
    response['Body'].close()
assert declared == inventory
for root in roots:
    expected = {k[len(root.name)+1:]: v for k, v in inventory['files'].items()
                if k.startswith(root.name + '/')}
    assert fa.inventory(root) == expected
after = closure()
for root in roots:
    shutil.rmtree(root)
shutil.rmtree(stage)
archive.unlink()
report = dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              roots=list(map(str, roots)), closure_before=before, closure_after=after,
              bucket=bucket, key=key, endpoint='https://nameless-bird-8772.int.exe.xyz',
              full_readback=actual, all_members_verified=True,
              original_allocated_bytes=allocated, tmp_before=usage_before,
              tmp_after=subprocess.check_output(['sudo', 'du', '-sx', '/tmp'], text=True).strip(),
              all_removed=all(not root.exists() for root in roots),
              scope='Preserves closed interrupted native stores and actual build files/binary. No terminal exit was recorded; no test verdict changes.')
(base / 'removal.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({k: report[k] for k in ('key', 'full_readback', 'original_allocated_bytes', 'tmp_before', 'tmp_after', 'all_removed')}))
