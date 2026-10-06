from pathlib import Path
import hashlib,json,subprocess,sys,io,tarfile,importlib.util,shutil
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
base='docs/scale/byte-bounded-audit-2026-10-05/24h-failed-1750';blob=lambda p:subprocess.check_output(['git','cat-file','blob',head+':'+base+'/'+p],cwd=repo)
meta=json.loads(blob('archive-verification.json'));declared=json.loads(blob('archive-manifest.json'));root=Path('/tmp/js-wf-journal-byte-bounded-explicit-routes-normal-2g-24h-20261005');raw=root/'originals.tar.gz'
assert closed.sha(raw)==meta['archive_sha256']==declared['archive_sha256'] and raw.stat().st_size==meta['archive_bytes'] and not raw.is_symlink()
blocks=[];combined=hashlib.sha256();total=0
for p in meta['parts']:
 data=blob(p['file']);assert len(data)==p['bytes'] and hashlib.sha256(data).hexdigest()==p['sha256'];blocks.append(data);combined.update(data);total+=len(data)
assert combined.hexdigest()==meta['archive_sha256'] and total==meta['archive_bytes']
actual={}
with tarfile.open(fileobj=io.BytesIO(b''.join(blocks)),mode='r|gz') as t:
 for member in t:
  if member.isdir():continue
  n=member.name.removeprefix('./');fixture_delta.safe_name(n);assert member.isfile() and n not in actual
  if n=='archive-manifest.json' and n not in declared['files']:continue
  actual[n]=fixture_delta.digest(t.extractfile(member))
assert set(actual)==set(declared['files'])
for n,d in declared['files'].items():assert actual[n]['sha256']==d
execution=root/'execution.json';assert actual['execution.json']=={'bytes':execution.stat().st_size,'sha256':closed.sha(execution)}
e=json.loads(execution.read_text());assert e['status']=='failed' and not Path('/proc',str(e['test_pid'])).exists()
fdcheck=closed.verify_no_open_originals(raw)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=Path('/tmp/js-wf-byte-soak-raw-recovery-20261006');out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
allocated=raw.stat().st_blocks*512;assert closed.sha(raw)==meta['archive_sha256'];raw.unlink()
r=dict(head=head,canonical=base,archive_sha256=meta['archive_sha256'],archive_bytes=total,all_git_parts_combined_and_full_inventory_verified=True,members=len(actual),execution_bound_to_canonical_and_sdk_absent=True,visible_fd_check=fdcheck,allocated_bytes_recovered=allocated,free_bytes=shutil.disk_usage(repo).free,scope='Only redundant raw originals.tar.gz removed. Failed donor stores/source/exes/metadata/cache/live and canonical Git archive retained; no NATS startup or change to failed verdict.')
(out/'recovery.json').write_text(json.dumps(r,indent=2)+'\n');print('RECOVERED',allocated,'FREE',r['free_bytes'],flush=True)
