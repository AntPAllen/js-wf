from pathlib import Path
import sys,json,subprocess,shutil,hashlib
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
root=Path('/tmp/js-wf-r5-retained-profile-restored-20261005');out=repo/'docs/scale/r5-profile-derived-store-preservation-2026-10-06'
preparation=Path('/tmp/js-wf-r5-profile-clone-preservation-preparation-20261006.json');prep=json.loads(preparation.read_text())
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==prep['head']
assert not (root/'lossless-manifest.json').exists() and not (root/'proof-delta.tar.gz').exists()
current={}
for p in (root/'copied-stores').rglob('*'):
 assert not p.is_symlink()
 if p.is_file():
  with p.open('rb') as f:sha=hashlib.file_digest(f,'sha256').hexdigest()
  current[p.relative_to(root/'copied-stores').as_posix()]=dict(bytes=p.stat().st_size,sha256=sha)
assert current==prep['current_copied_store_files']
shutil.copy2(preparation,root/'clone-preservation-preparation.json');shutil.copy2(__file__,root/'executed-clone-preservation.py')
proof=fixture_delta.capture(root,out,repo,head,prep['base_canonical_metadata'],'copied-stores/','originals/TestStreamingAuditR5LargeInterruptedPull/cluster/')
shutil.copy2(preparation,out/'preparation.json');shutil.copy2(__file__,out/'executed-preservation.py')
verification=fixture_delta.verify(root/'proof-delta.tar.gz',repo)
(out/'independent-delta-verification.json').write_text(json.dumps(verification,indent=2)+'\n')
print(json.dumps(proof,indent=2),flush=True)
