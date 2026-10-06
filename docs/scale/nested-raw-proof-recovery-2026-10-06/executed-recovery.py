from pathlib import Path
import subprocess,sys,json,hashlib,tarfile,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
groups=[('watch-observed-journal-24h','watch-observed-journal-24h-2026-10-05/failed-3140'),('continuous-byte-journal-24h','continuous-byte-journal-24h-2026-10-05/failed-1040'),('parallel-latency-journal-10m','parallel-final-latency-2026-10-06/live-journal-ten-minute'),('chunked-journal-10m','chunked-callback-audit-2026-10-06/live-journal-ten-minute'),('cached-latency-journal-10m','cached-final-latency-2026-10-06/live-journal-ten-minute'),('r5-profile-publication','r5-audit-profile-2026-10-05')]
records=[];fdchecks=[];archives=[];inventories={}
for name,dest in groups:
 canonical='docs/scale/'+dest
 meta=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/archive-verification.json'],cwd=repo))
 outer=fixture_delta.read_base(repo,head,canonical+'/archive-verification.json')
 roots=[Path('/tmp/js-wf-'+name+'-20261005'),Path('/tmp/js-wf-'+name+'-20261006'),Path('/tmp/js-wf-'+name+'-terminal-proof-20261005'),Path('/tmp/js-wf-'+name+'-proof-20261006')]
 for root in roots:
  if not root.exists():continue
  execution=root/'execution.json'
  if execution.exists() and 'execution.json' in outer:
   with execution.open('rb') as f:assert fixture_delta.digest(f)==outer['execution.json']
   e=json.loads(execution.read_text());pid=e.get('pid',e.get('test_pid'));assert pid and not Path('/proc',str(pid)).exists()
  for filename in ['proof.tar.gz','originals.tar.gz']:
   raw=root/filename
   if not raw.is_file():continue
   expected=({'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']} if filename=='proof.tar.gz' else outer.get(filename))
   assert expected is not None
   with raw.open('rb') as f:assert fixture_delta.digest(f)==expected
   assert not raw.is_symlink()
   if filename=='originals.tar.gz' and name not in inventories:
    actual={};declared=None
    with tarfile.open(raw,'r|gz') as archive:
     for member in archive:
      if member.isdir():continue
      key=member.name.removeprefix('./');fixture_delta.safe_name(key);assert member.isfile() and key not in actual
      data=archive.extractfile(member)
      if key=='archive-manifest.json':assert declared is None;declared=json.load(data)
      else:actual[key]=fixture_delta.digest(data)
    if declared is None:declared=json.loads((root/'archive-manifest.json').read_text())
    assert set(actual)==set(declared['files'])
    for key,value in declared['files'].items():assert actual[key]['sha256']==value
    assert declared['archive_sha256']==expected['sha256']
    # Preserve canonical-bound inner inventory for subsequent fresh copies.
    inventories[name]=dict(archive_sha256=expected['sha256'],files=declared['files'],canonical_outer=canonical,revision=head)
   fdchecks.append(closed.verify_no_open_originals(raw));records.append(dict(path=str(raw),canonical_outer=canonical,member=filename,bytes=expected['bytes'],sha256=expected['sha256'],allocated_bytes=raw.stat().st_blocks*512))
 archives.append(dict(canonical=canonical,sha256=meta['archive_sha256'],members=len(outer)))
 print('VERIFIED',canonical,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=Path('/tmp/js-wf-nested-raw-proof-recovery-20261006');out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
for name,m in inventories.items():(out/(name+'-inner-manifest.json')).write_text(json.dumps(m,indent=2)+'\n')
for record in records:
 p=Path(record['path']);assert closed.sha(p)==record['sha256'];p.unlink()
for record in records:assert not Path(record['path']).exists()
report=dict(head=head,pushed_main_matches=True,canonical_archives_read_in_full=archives,inner_member_inventories_checked=list(inventories),duplicates=records,visible_fd_checks=fdchecks,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only redundant raw proof/archive files removed after canonical full Git and inner-member verification. Current native execution records where present bound to canonical with absent SDK PIDs. Publication archives are immutable verified byte duplicates, not runtime closure claims. Original stores, source/executable/metadata/cache/live and all canonical Git proofs retained. No NATS startup.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
