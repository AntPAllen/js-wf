from pathlib import Path
import json,sys,subprocess,shutil,importlib.util,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
base=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-journal-ten-minute'
meta=json.loads((base/'archive-verification.json').read_bytes());manifest=json.loads((base/'fixture-inventory.json').read_bytes());receipt=json.loads((base/'s3-readback.json').read_bytes())
root=Path('/tmp/js-wf-bulk-soak-terminal-review-control-20261006');root.mkdir();raw=root/'proof.tar.gz';restored=root/'restored';watch=root/'observer'
command=['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800','--output',str(raw),receipt['archive']['url']]
subprocess.run(command,input=s3.credentials(),check=True)
restoration=fixture_archive.restore(raw,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']),manifest,restored)
watch.mkdir()
for p in restored.glob('watch-*'):
 if p.is_file():shutil.copy2(p,watch/p.name[len('watch-'):])
shutil.copytree(restored/'actual-server-executables',watch/'server-executables')
print('FRESH_CONTROL_FILES_RESTORED',restoration['files'],flush=True)
script=repo/'scripts/review-bulk-soak-terminal.py'
args=[sys.executable,str(script),'--root',str(restored),'--watch',str(watch),'--repo',str(repo),'--output',str(root/'review.json'),'--original-root','/tmp/js-wf-bulk-journal-ten-minute-joined-20261006','--duration','10m','--expected-source','ca7a1fa658adcd3bdd0ead2daddb6fcf7f9f1129','--expected-sdk-pid','3239290']
with (root/'review.log').open('w') as log:r=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT)
(root/'control-state.json').write_text(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),restoration=restoration,command=args,reviewer_sha256=fixture_archive.digest(script.open('rb'))['sha256'],exit_code=r.returncode,restored_census_unchanged=fixture_archive.inventory(restored)==manifest['files'],scope='Fresh file restore/read-only Python review control of accepted original10m component; no NATS or native SDK started. Does not qualify24h.'),indent=2)+'\n')
print('READ_ONLY_TERMINAL_CONTROL',r.returncode,flush=True)
