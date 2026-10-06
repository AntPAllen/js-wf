from pathlib import Path
import subprocess,sys,json,hashlib,shutil,importlib.util,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
root=Path('/tmp/js-wf-bulk-latency-cohort-20261006');copy=root/'copied-stores';canonical='docs/scale/bulk-final-latency-preparation-2026-10-06/retained-cohort'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
git=lambda name:subprocess.check_output(['git','cat-file','blob',head+':'+canonical+'/'+name],cwd=repo)
meta=json.loads(git('archive-verification.json'));manifest=fixture_delta.read_base(repo,head,canonical+'/archive-verification.json');assert manifest==json.loads((root/'archive-manifest.json').read_text())
receipt_path='docs/scale/s3-proof-offload-2026-10-06/bulk-cohort-failure-readback.json';receipt=json.loads(subprocess.check_output(['git','cat-file','blob',head+':'+receipt_path],cwd=repo));assert receipt['archive']['full_readback']=={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
for name,record in manifest.items():
 p=root/name;assert p.is_file() and not p.is_symlink() and p.stat().st_size==record['bytes'] and closed.sha(p)==record['sha256'],name
expected={name.removeprefix('copied-stores/'):record for name,record in manifest.items() if name.startswith('copied-stores/')};actual={};allocated=0
for p in copy.rglob('*'):
 assert not p.is_symlink()
 if p.is_file():
  assert p.stat().st_nlink==1;actual[p.relative_to(copy).as_posix()]={'bytes':p.stat().st_size,'sha256':closed.sha(p)};allocated+=p.stat().st_blocks*512
assert actual==expected
execution=json.loads((root/'execution.json').read_text());assert execution['exit_code']==1 and not Path('/proc',str(execution['pid'])).exists()
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for s in servers:assert not Path('/proc',str(s['host_pid'])).exists()
ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else [];mounts=[]
for c in running:
 for m in c['Mounts']:
  source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
  mounts.append({'id':c['Id'],'source':str(source),'destination':m['Destination']})
fd=closed.verify_no_open_originals(root)
raw=root/'proof.tar.gz';assert closed.sha(raw)==meta['archive_sha256'];raw_record={'path':str(raw),'bytes':raw.stat().st_size,'allocated_bytes':raw.stat().st_blocks*512,'sha256':meta['archive_sha256']}
parts=[]
for part in meta['parts']:
 path=repo/canonical/part['file'];assert closed.sha(path)==part['sha256'] and path.stat().st_size==part['bytes'];closed.verify_no_open_originals(path)
 parts.append(dict(relative=path.relative_to(repo).as_posix(),allocated_bytes=path.stat().st_blocks*512,sha256=part['sha256']))
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=Path('/tmp/js-wf-bulk-cohort-copy-recovery-20261006');out.mkdir();shutil.copy2(__file__,out/'executed-recovery.py')
shutil.rmtree(copy);raw.unlink();patterns=repo/'.git/info/sparse-checkout';before=patterns.read_text();(out/'patterns-before.txt').write_text(before);patterns.write_text(before+''.join('!/'+p['relative']+'\n' for p in parts));subprocess.run(['git','sparse-checkout','reapply'],cwd=repo,check=True)
for name,record in manifest.items():
 if name.startswith('copied-stores/'):continue
 p=root/name;assert p.stat().st_size==record['bytes'] and closed.sha(p)==record['sha256']
for p in parts:assert not (repo/p['relative']).exists() and hashlib.sha256(subprocess.check_output(['git','cat-file','blob',head+':'+p['relative']],cwd=repo)).hexdigest()==p['sha256']
r=dict(head=head,pushed_main_matches=True,canonical=canonical,complete_archive_member_and_part_readback=True,s3_receipt=receipt_path,sdk_and_five_observed_servers_closed=True,copied_store_files=len(actual),all_current_captured_files_before_and_remaining_files_after_match=True,current_running_mounts=mounts,visible_fd_check=fd,removed_disposable_copy=str(copy),raw_duplicate=raw_record,working_part_duplicates=parts,allocated_bytes_recovered=allocated+raw_record['allocated_bytes']+sum(p['allocated_bytes'] for p in parts),free_bytes=shutil.disk_usage(repo).free,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Only canonically preserved closed disposable copied-stores and exact raw/worktree archive duplicates removed. Original failed-soak donor, primary source/exes/metadata, caches, canonical Git/S3 proof and live stores retained. Failed verdict unchanged; no NATS startup.')
(out/'recovery.json').write_text(json.dumps(r,indent=2)+'\n');print('RECOVERED',r['allocated_bytes_recovered'],'FREE',r['free_bytes'],flush=True)
