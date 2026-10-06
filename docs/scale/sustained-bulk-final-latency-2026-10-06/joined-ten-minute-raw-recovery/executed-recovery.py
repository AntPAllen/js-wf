from pathlib import Path
import subprocess,json,hashlib,sys,shutil,importlib.util,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
root=Path('/tmp/js-wf-bulk-journal-ten-minute-joined-20261006');raw=Path('/tmp/js-wf-bulk-journal-ten-minute-joined-complete-20261006.tar.gz');canonical='docs/scale/sustained-bulk-final-latency-2026-10-06/joined-journal-ten-minute'
blob=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
meta=json.loads(blob('archive-verification.json'));manifest=json.loads(blob('fixture-inventory.json'));receipt=json.loads(blob('s3-readback.json'))
assert raw.stat().st_size==meta['archive_bytes'] and closed.sha(raw)==meta['archive_sha256']
assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
assert receipt['archive']['full_readback']==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
for name,key in [('archive-verification.json','metadata'),('fixture-inventory.json','inventory')]:assert receipt[key]['full_readback']==dict(bytes=len(blob(name)),sha256=hashlib.sha256(blob(name)).hexdigest())
e=json.loads((root/'execution.json').read_text());assert e['status']=='row_verified'
review=json.loads((root/'independent-review.json').read_text());assert review['observed_processes_closed'] and review['native_terminal']['Action']=='pass'
assert not Path('/proc',str(review['actual_sdk_pid'])).exists()
servers=[json.loads(x) for x in (root/'watch-servers.jsonl').read_text().splitlines()];assert len(servers)==24
for server in servers:assert not Path('/proc',str(server['pid'])).exists()
ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
for c in running:
 for m in c['Mounts']:
  source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
limits=[]
for p in Path('/proc').glob('[0-9]*'):
 try:
  exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
  assert not exe.is_relative_to(root) and not any(str(root).encode() in arg for arg in args)
 except PermissionError:limits.append(str(p))
 except (FileNotFoundError,ProcessLookupError):pass
fd=closed.verify_no_open_originals(root);rawfd=closed.verify_no_open_originals(raw)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-ten-minute-raw-recovery';out.mkdir();shutil.copyfile(__file__,out/'executed-recovery.py')
allocated=raw.stat().st_blocks*512;assert closed.sha(raw)==meta['archive_sha256'];raw.unlink()
assert fixture_archive.inventory(root)==manifest['files']
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),canonical=canonical,archive_sha256=meta['archive_sha256'],archive_bytes=meta['archive_bytes'],all_committed_archive_members_and_full_current_fixture_inventory_verified=True,committed_complete_s3_archive_metadata_inventory_readback=True,actual_sdk_and_24_observed_servers_closed=True,current_root_process_and_running_docker_mount_scope_clear=True,unobservable_processes=limits,visible_fd_checks=[fd,rawfd],removed_raw_duplicate=str(raw),allocated_bytes_recovered=allocated,free_bytes=shutil.disk_usage(repo).free,all_current_original_store_source_executable_metadata_bytes_modes_mtimes_unchanged=True,scope='Only redundant completely S3-preserved local archive removed. Every original donor/store/source/executable/metadata file, caches and complete Git inventories/receipts retained. No SDK/NATS startup, verdict change or independent provider durability claim.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',allocated,'FREE',report['free_bytes'],flush=True)
