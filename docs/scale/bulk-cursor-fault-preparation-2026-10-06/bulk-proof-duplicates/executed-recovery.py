from pathlib import Path
import subprocess,json,hashlib,sys,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
items=[('/tmp/js-wf-bulk-latency-cohort-whole-state-20261006','docs/scale/state-admission-deadlines-2026-10-06/native-bulk-whole-state',True)]
records=[];limits=[]
for dirname,canonical,delta in items:
 root=Path(dirname);git=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
 meta=json.loads(git('archive-verification.json'));raw=root/('proof-delta.tar.gz' if delta else 'proof.tar.gz')
 assert raw.is_file() and not raw.is_symlink() and raw.stat().st_size==meta['archive_bytes'] and closed.sha(raw)==meta['archive_sha256']
 e=json.loads((root/'execution.json').read_text());assert not Path('/proc',str(e['pid'])).exists()
 if delta:
  assert fixture_delta.verify(raw,repo)['logical_files']==meta['logical_files']
  manifest=json.loads(git('lossless-manifest.json'));assert manifest==json.loads((root/'lossless-manifest.json').read_text())
  expected=manifest['files']['execution.json']
 else:
  manifest=fixture_delta.read_base(repo,head,canonical+'/archive-verification.json');assert manifest==json.loads((root/'archive-manifest.json').read_text());expected=manifest['execution.json']
 assert expected['bytes']==(root/'execution.json').stat().st_size and expected['sha256']==closed.sha(root/'execution.json')
 limits.append(closed.verify_no_open_originals(raw));records.append(dict(path=str(raw),bytes=raw.stat().st_size,allocated_bytes=raw.stat().st_blocks*512,sha256=meta['archive_sha256'],kind='raw',canonical=canonical))
 combined=hashlib.sha256();size=0
 for part in meta['parts']:
  data=git(part['file']);assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'];combined.update(data);size+=len(data)
  path=repo/canonical/part['file']
  if path.exists():
   assert path.is_file() and not path.is_symlink() and path.stat().st_size==part['bytes'] and closed.sha(path)==part['sha256']
   limits.append(closed.verify_no_open_originals(path));records.append(dict(path=str(path),bytes=path.stat().st_size,allocated_bytes=path.stat().st_blocks*512,sha256=part['sha256'],kind='worktree',canonical=canonical,relative=path.relative_to(repo).as_posix()))
 assert size==meta['archive_bytes'] and combined.hexdigest()==meta['archive_sha256']
 print('VERIFIED',canonical,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=Path('/tmp/js-wf-bulk-whole-state-proof-duplicate-recovery-20261006');out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
for record in records:
 if record['kind']=='raw':p=Path(record['path']);assert closed.sha(p)==record['sha256'];p.unlink()
patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before);patterns.write_text(before+''.join('!/'+r['relative']+'\n' for r in records if r['kind']=='worktree'));subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for record in records:
 assert not Path(record['path']).exists()
 if record['kind']=='worktree':assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',head+':'+record['relative']],cwd=repo)).hexdigest()==record['sha256']
report=dict(head=head,pushed_main_matches=True,complete_canonical_archives_or_base_plus_delta_verified=True,closed_sdk_execution_bound_to_canonical=True,duplicates=records,visible_fd_checks=limits,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only verified raw/worktree proof duplicates removed; canonical Git base/delta, original/failed fixtures, sources/exes/metadata/caches/live stores retained. No NATS startup.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
