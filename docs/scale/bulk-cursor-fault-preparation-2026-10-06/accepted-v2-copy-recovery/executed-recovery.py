from pathlib import Path
import subprocess,json,hashlib,sys,shutil,importlib.util,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
root=Path('/tmp/js-wf-bulk-latency-cohort-owner-restart-v2-20261006');copy=root/'copied-stores';raw=Path('/tmp/js-wf-bulk-owner-restart-v2-complete-20261006.tar.gz');canonical='docs/scale/bulk-cursor-fault-preparation-2026-10-06/native-real87920-owner-restart-v2'
blob=lambda file:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+file],cwd=repo)
meta=json.loads(blob('archive-verification.json'));manifest=json.loads(blob('fixture-inventory.json'));receipt=json.loads(blob('s3-readback.json'))
assert raw.stat().st_size==meta['archive_bytes'] and closed.sha(raw)==meta['archive_sha256']
assert fixture_archive.verify(raw)==manifest and fixture_archive.inventory(root)==manifest['files']
assert receipt['archive']['full_readback']==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
for name,key in [('archive-verification.json','metadata'),('fixture-inventory.json','inventory')]:assert receipt[key]['full_readback']==dict(bytes=len(blob(name)),sha256=hashlib.sha256(blob(name)).hexdigest())
expected={name[len('copied-stores/'):]:rec for name,rec in manifest['files'].items() if name.startswith('copied-stores/')};actual={};allocated=0
for p in copy.rglob('*'):
 assert not p.is_symlink()
 if p.is_file():
  assert p.stat().st_nlink==1;actual[p.relative_to(copy).as_posix()]=dict(bytes=p.stat().st_size,sha256=closed.sha(p));allocated+=p.stat().st_blocks*512
assert actual=={name:{k:rec[k] for k in ['bytes','sha256']} for name,rec in expected.items()}
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc',str(e['pid'])).exists()
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==6;closures=[]
for server in servers:
 assert not Path('/proc',str(server['host_pid'])).exists()
 cid=server['container']['Id'];r=subprocess.run(['docker','inspect',cid],capture_output=True,text=True)
 if r.returncode:assert 'no such object' in r.stderr.lower();state='removed'
 else:
  item=json.loads(r.stdout)[0];assert not item['State']['Running'] and not item['State']['Restarting'];state=item['State']
 closures.append(dict(id=cid,recorded_pid=server['host_pid'],current=state))
ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
for c in running:
 for mount in c['Mounts']:
  source=Path(mount['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
fd=closed.verify_no_open_originals(root);raw_fd=closed.verify_no_open_originals(raw)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/bulk-cursor-fault-preparation-2026-10-06/accepted-v2-copy-recovery';out.mkdir();shutil.copyfile(__file__,out/'executed-recovery.py')
raw_allocated=raw.stat().st_blocks*512;assert closed.sha(raw)==meta['archive_sha256'];raw.unlink();shutil.rmtree(copy)
remaining=fixture_archive.inventory(root);assert remaining=={name:rec for name,rec in manifest['files'].items() if not name.startswith('copied-stores/')}
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),canonical=canonical,complete_archive_and_full_current_file_inventory_verified=True,committed_complete_s3_archive_metadata_inventory_readback=True,copied_store_files=len(actual),sdk_and_six_actual_server_pids_closed=True,current_historical_containers=closures,visible_fd_checks=[fd,raw_fd],removed_copy=str(copy),removed_raw_duplicate=str(raw),copied_store_allocated_bytes=allocated,raw_allocated_bytes=raw_allocated,allocated_bytes_recovered=allocated+raw_allocated,free_bytes=shutil.disk_usage(repo).free,all_remaining_captured_primary_source_executable_metadata_files_unchanged=True,scope='Only completely S3-preserved closed disposable copied stores and redundant local complete archive removed; original donors, primary source/exes/metadata/caches and Git complete inventories/hash/readback receipts retained. No NATS/SDK startup, historical verdict change or independent S3 durability guarantee.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',allocated+raw_allocated,'FREE',report['free_bytes'],flush=True)
