from pathlib import Path
import subprocess,json,hashlib,sys,importlib.util,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
items=[('/tmp/js-wf-byte-refill-corrected-20261005','docs/scale/byte-refill-diagnosis-2026-10-05/first-continuous'),('/tmp/js-wf-byte-refill-final-controls-20261005','docs/scale/byte-refill-diagnosis-2026-10-05/final-controls')]
records=[]
for dirname,canonical in items:
 root=Path(dirname);raw=root/'proof.tar.gz';meta=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/archive-verification.json'],cwd=repo))
 assert raw.is_file() and not raw.is_symlink() and raw.stat().st_size==meta['archive_bytes'] and closed.sha(raw)==meta['archive_sha256']
 inventory=fixture_delta.read_base(repo,head,canonical+'/archive-verification.json');assert inventory==json.loads((root/'archive-manifest.json').read_text())
 frames=[]
 for name in ['normal-actual.json','race-actual.json']:
  p=root/name;expected=inventory[name];assert p.stat().st_size==expected['bytes'] and closed.sha(p)==expected['sha256']
  actual=json.loads(p.read_text());assert not Path('/proc',str(actual['pid'])).exists()
  exe=Path(actual['exe']);assert exe.is_relative_to(root);relative=exe.relative_to(root).as_posix();expected_exe=inventory[relative]
  assert exe.is_file() and closed.sha(exe)==actual['exe_sha256']==expected_exe['sha256'] and exe.stat().st_size==expected_exe['bytes']
  frames.append(dict(actual=actual,canonical_member=name,executable_member=relative))
 fd=closed.verify_no_open_originals(raw)
 records.append(dict(path=str(raw),canonical=canonical,sha256=meta['archive_sha256'],bytes=raw.stat().st_size,allocated_bytes=raw.stat().st_blocks*512,logical_members=len(inventory),closed_actual_sdk_frames=frames,visible_fd_check=fd))
 print('VERIFIED',canonical,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=Path('/tmp/js-wf-byte-control-proof-duplicate-recovery-20261006');out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
for r in records:
 p=Path(r['path']);assert closed.sha(p)==r['sha256'];p.unlink()
report=dict(head=head,pushed_main_matches=True,all_canonical_parts_members_and_concat_verified=True,duplicates=records,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only two hash-identical raw proof archive duplicates removed; complete canonical Git archive, primary original stores/source/exes/metadata/caches/live retained. Closed actual SDK metadata/executable bytes bound to canonical archive. No NATS startup or verdict changes.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
