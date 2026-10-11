"""Download fresh verified S3 bytes, restore all files, rerun unchanged gates."""
import datetime, hashlib, importlib.util, json, os, shutil, subprocess, sys
from pathlib import Path
from capture import root, base, repo
spec=importlib.util.spec_from_file_location('s3_copy',repo/'scripts/offload-proof-to-s3.py')
s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
remote=json.loads((base/'s3-readback.json').read_text())
meta=json.loads((base/'archive-verification.json').read_text())
expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
assert remote['archive']['full_readback']==expected
archive=Path('/home/exedev/closed-classified-entry100000-native-20261011.tar.gz')
download=Path('/home/exedev/closed-classified-entry100000-readback-20261011.tar.gz')
recovered=Path('/home/exedev/closed-classified-entry100000-recovery-20261011')
assert not download.exists() and not recovered.exists()
assert shutil.disk_usage(repo).free > expected['bytes'] + 3365343276 + (256<<20)
cmd=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800','--output',str(download),'--write-out','%{http_code}',remote['archive']['url']]
r=subprocess.run(cmd,input=s3.credentials(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert r.returncode==0 and r.stdout.strip()==b'200',(r.returncode,r.stdout)
with download.open('rb') as stream:actual=s3.digest(stream)
assert actual==expected
# Replace only this task's staging archive after validating the fresh download.
os.replace(download,archive)
(base/'fresh-download.json').write_text(json.dumps(dict(url=remote['archive']['url'],http_status=200,full_body=actual,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2)+'\n')
with (base/'recovery.json').open('x') as output:
 subprocess.run([sys.executable,str(repo/'scripts/restore-full-fixture-proof.py'),'--archive',str(archive),'--metadata',str(base/'archive-verification.json'),'--inventory',str(base/'fixture-inventory.json'),'--destination',str(recovered)],stdout=output,check=True)
subprocess.run([sys.executable,str(repo/'docs/scale/graph-classified-entry-campaign-2026-10-11/review.py'),str(root),'--native-root',str(recovered),'--output',str(base/'restored-review.json')],check=True)
