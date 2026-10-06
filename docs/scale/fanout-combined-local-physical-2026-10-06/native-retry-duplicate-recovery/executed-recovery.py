from pathlib import Path
import sys,json,hashlib,subprocess,os,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','show',head+':scripts/fixture_delta.py'],cwd=repo)
cases=[('fanout-combined-local-physical-retry','fanout-combined-local-physical-2026-10-06/native-retry')]
out=Path('/tmp/js-wf-fanout-native-retry-duplicate-recovery-20261006');out.mkdir()
(out/'executed-recovery.py').write_bytes(Path(__file__).read_bytes())
records=[];parts=[];limits=set()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def nofds(paths):
 for proc in Path('/proc').glob('[0-9]*'):
  for task in proc.glob('task/*'):
   try:fds=list((task/'fd').iterdir())
   except (OSError,PermissionError):limits.add(str(task/'fd'));continue
   for fd in fds:
    try:target=os.readlink(fd)
    except FileNotFoundError:continue
    except PermissionError:limits.add(str(fd));continue
    assert target not in paths,('open proof duplicate',str(fd),target)
for name,canonical in cases:
 root=Path('/tmp/js-wf-'+name+'-20261006');path=root/'proof.tar.gz'
 metadata='docs/scale/'+canonical+'/archive-verification.json'
 expected=fixture_delta.read_base(repo,head,metadata)
 manifest=json.loads(subprocess.check_output(['git','show',head+':'+metadata],cwd=repo))
 e=json.loads((root/'execution.json').read_text())
 assert e['status'] in ('passed','failed') and not Path('/proc',str(e['pid'])).exists()
 assert dict(bytes=(root/'execution.json').stat().st_size,sha256=sha(root/'execution.json'))==expected['execution.json']
 for field in ('observed-servers.json','observed-workers.json'):
  if (root/field).exists():
   for observed in json.loads((root/field).read_text()):assert not Path('/proc',str(observed['pid'])).exists()
 assert json.loads((root/'archive-manifest.json').read_text())==expected
 if path.exists():
  assert path.is_file() and not path.is_symlink() and path.stat().st_size==manifest['archive_bytes'] and sha(path)==manifest['archive_sha256']
  nofds({str(path)})
  records.append(dict(path=str(path),bytes=path.stat().st_size,allocated_bytes=path.stat().st_blocks*512,sha256=manifest['archive_sha256'],canonical_metadata=metadata,canonical_all_parts_concat_members_manifest_verified=True,sdk_observed_processes_closed=True,visible_fds_checked=True))
 for part in manifest['parts']:
  rel=str(Path(metadata).parent/part['file']);p=repo/rel
  if p.exists():
   assert p.is_file() and not p.is_symlink() and p.stat().st_size==part['bytes'] and sha(p)==part['sha256']
   nofds({str(p)})
   parts.append(dict(path=rel,bytes=part['bytes'],allocated_bytes=p.stat().st_blocks*512,sha256=part['sha256'],git_blob=subprocess.check_output(['git','rev-parse',head+':'+rel],cwd=repo,text=True).strip()))
 print('VERIFIED',name,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
# Only duplicates listed above are removed after all complete canonical reviews.
for record in records:
 path=Path(record['path']);assert sha(path)==record['sha256'];path.unlink()
patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before)
patterns.write_text(before+''.join('!/'+p['path']+'\n' for p in parts));subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for part in parts:assert not (repo/part['path']).exists() and subprocess.check_output(['git','rev-parse',head+':'+part['path']],cwd=repo,text=True).strip()==part['git_blob']
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),raw_archives=records,working_tree_parts=parts,unobservable_descriptors=sorted(limits),allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records+parts),free_bytes=shutil.disk_usage(repo).free,scope='Only verified canonical duplicate raw archives and worktree parts; canonical Git, manifests, original donor/store/source/executable/caches and live stores retained. Raw archive reconstruction from canonical parts required before older closed-original verifier reuse.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
