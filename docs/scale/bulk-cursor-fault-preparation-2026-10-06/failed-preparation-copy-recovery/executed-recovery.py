from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
root=Path('/tmp/js-wf-bulk-latency-cohort-owner-restart-20261006');copy=root/'copied-stores';canonical='docs/scale/bulk-cursor-fault-preparation-2026-10-06/native-real87920-owner-restart'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
git=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
meta=json.loads(git('archive-verification.json'));manifest=json.loads(git('lossless-manifest.json'))
assert manifest==json.loads((root/'lossless-manifest.json').read_text())
combined=hashlib.sha256();total=0
for part in meta['parts']:
 data=git(part['file']);assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'];combined.update(data);total+=len(data)
assert total==meta['archive_bytes'] and combined.hexdigest()==meta['archive_sha256']==closed.sha(root/'proof-delta.tar.gz')
virtual=fixture_delta.verify(root/'proof-delta.tar.gz',repo);assert virtual['logical_files']==len(manifest['files'])==meta['logical_files']
# Every current captured file, including all clone bytes, must still match.
for name,record in manifest['files'].items():
 p=root/name;assert p.is_file() and not p.is_symlink() and p.stat().st_size==record['bytes'] and closed.sha(p)==record['sha256'],name
expected={name[len('copied-stores/'):]:record for name,record in manifest['files'].items() if name.startswith('copied-stores/')}
actual={};allocated=0
for p in copy.rglob('*'):
 assert not p.is_symlink()
 if p.is_file():
  assert p.stat().st_nlink==1
  actual[p.relative_to(copy).as_posix()]=dict(bytes=p.stat().st_size,sha256=closed.sha(p));allocated+=p.stat().st_blocks*512
assert actual=={name:{k:record[k] for k in ('bytes','sha256')} for name,record in expected.items()}
execution=json.loads((root/'execution.json').read_text());assert execution['exit_code']==1 and execution['status']=='failed' and not Path('/proc',str(execution['pid'])).exists()
closures=[]
for c in json.loads((root/'actual-containers/actual-servers.json').read_text()):
 assert not Path('/proc',str(c['host_pid'])).exists()
 cid=c['container']['Id'];r=subprocess.run(['docker','inspect',cid],capture_output=True,text=True)
 if r.returncode:assert 'no such object' in r.stderr.lower();state='removed'
 else:
  current=json.loads(r.stdout)[0];assert not current['State']['Running'] and not current['State']['Restarting'];state=current['State']
 closures.append(dict(id=cid,recorded_pid=c['host_pid'],current=state))
ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
mounts=[]
for c in running:
 for mount in c['Mounts']:
  source=Path(mount['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
  mounts.append(dict(id=c['Id'],source=str(source),destination=mount['Destination']))
fd=closed.verify_no_open_originals(root)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
raw=root/'proof-delta.tar.gz';raw_allocated=raw.stat().st_blocks*512;assert closed.sha(raw)==meta['archive_sha256'];raw.unlink()
out=repo/'docs/scale/bulk-cursor-fault-preparation-2026-10-06/failed-preparation-copy-recovery';out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
shutil.rmtree(copy);assert not copy.exists()
for name,record in manifest['files'].items():
 if not name.startswith('copied-stores/'):
  p=root/name;assert p.stat().st_size==record['bytes'] and closed.sha(p)==record['sha256']
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),canonical_delta=canonical,canonical_delta_sha256=meta['archive_sha256'],all_delta_parts_and_complete_base_plus_delta_verified=True,virtual_tree=virtual,all_current_clone_files_match_canonical_preservation=True,copied_store_files=len(actual),sdk_and_observed_server_pids_closed=True,current_historical_containers=closures,current_running_mounts=mounts,visible_fd_check=fd,removed_copy=str(copy),allocated_bytes_recovered=allocated+raw_allocated,raw_duplicate_removed=str(raw),raw_duplicate_allocated_bytes=raw_allocated,free_bytes=shutil.disk_usage(repo).free,all_remaining_captured_source_executable_metadata_original_files_unchanged=True,scope='Only complete canonically preserved disposable copied-stores and their hash-identical redundant raw delta removed; immutable Git base/delta, original donor/failed native fixtures, primary metadata/source/exes/caches and live stores retained. Historical verdicts unchanged. No NATS startup.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',allocated,'FREE',report['free_bytes'],flush=True)
