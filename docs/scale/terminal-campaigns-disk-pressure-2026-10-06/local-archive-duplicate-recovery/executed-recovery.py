from pathlib import Path
import subprocess,json,hashlib,sys,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
records=[]
for name,root in [('journal',Path('/tmp/js-wf-chunked-journal-24h-20261006')),('million',Path('/tmp/js-wf-million-candidate-24h-20261005'))]:
 canonical='docs/scale/terminal-campaigns-disk-pressure-2026-10-06/complete-'+name
 blob=lambda file:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+file],cwd=repo)
 meta=json.loads(blob('archive-verification.json'));manifest=json.loads(blob('fixture-inventory.json'));receipt=json.loads(blob('s3-readback.json'))
 raw=Path('/tmp/js-wf-terminal-campaign-archives-20261006')/(name+'.tar.gz');assert not raw.is_symlink()
 assert raw.stat().st_size==meta['archive_bytes'] and closed.sha(raw)==meta['archive_sha256']
 assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
 assert receipt['archive']['full_readback']==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert receipt['metadata']['full_readback']==dict(bytes=len(blob('archive-verification.json')),sha256=hashlib.sha256(blob('archive-verification.json')).hexdigest())
 assert receipt['inventory']['full_readback']==dict(bytes=len(blob('fixture-inventory.json')),sha256=hashlib.sha256(blob('fixture-inventory.json')).hexdigest())
 fd=closed.verify_no_open_originals(raw)
 records.append(dict(path=str(raw),canonical=canonical,sha256=meta['archive_sha256'],bytes=raw.stat().st_size,allocated_bytes=raw.stat().st_blocks*512,complete_archive_and_current_primary_file_inventory_verified=True,s3_full_readback_committed=True,visible_fd_check=fd))
 print('VERIFIED',name,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/terminal-campaigns-disk-pressure-2026-10-06/local-archive-duplicate-recovery';out.mkdir();shutil.copyfile(__file__,out/'executed-recovery.py')
for r in records:
 p=Path(r['path']);assert closed.sha(p)==r['sha256'];p.unlink()
report=dict(head=head,pushed_main_matches=True,duplicates=records,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only complete readback-verified S3 archive local duplicates removed. Primary original stores/source/exes/metadata/partial artifacts and Git inventories/hash/readback receipts remain. No SDK/NATS startup or verdict change; no independent S3 durability guarantee.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
