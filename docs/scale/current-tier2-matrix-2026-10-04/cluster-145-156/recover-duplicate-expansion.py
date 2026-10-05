import pathlib,json,hashlib,subprocess,tarfile,zipfile,os,shutil
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-tier2-current-cluster145-156-37149506857');out=repo/'docs/scale/current-tier2-matrix-2026-10-04/cluster-145-156';manifest=json.load(open(out/'manifest.json'))
remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert remote==subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
h=hashlib.sha256()
for part in manifest['parts']:
 p=out/part['path'];data=p.read_bytes();assert hashlib.sha256(data).hexdigest()==part['sha256'];assert subprocess.check_output(['git','show',remote+':'+str(p.relative_to(repo))],cwd=repo)==data;h.update(data)
assert h.hexdigest()==manifest['archive_sha256']
with tarfile.open(root/'proof.tar.gz') as t:
 for name,entry in manifest['files'].items():assert hashlib.sha256(t.extractfile(name).read()).hexdigest()==entry['sha256']
with zipfile.ZipFile(root/'raw.zip') as z:
 for info in z.infolist():assert hashlib.sha256(z.read(info)).hexdigest()==manifest['files']['artifact/'+info.filename]['sha256']
raw=root/'raw';count=0;allocated=0
for p in raw.rglob('*'):
 if p.is_file():assert hashlib.sha256(p.read_bytes()).hexdigest()==manifest['files']['artifact/'+str(p.relative_to(raw))]['sha256'];count+=1;allocated+=p.stat().st_blocks*512
assert count==72
open_targets=[]
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 try:
  for fd in (proc/'fd').iterdir():
   try:target=os.readlink(fd)
   except OSError:continue
   if target==str(raw) or target.startswith(str(raw)+'/'):open_targets.append(str(fd))
 except OSError:pass
assert not open_targets
shutil.rmtree(raw)
report={'published_commit':remote,'raw_files_verified':count,'exclusive_allocated_bytes_recovered':allocated,'archive_members_zip_and_published_parts_verified':True,'visible_target_file_descriptors':open_targets,'removed_only_duplicate_raw_expansion':True,'retained':'Original ZIP, canonical proof, first/corrected proof provenance, model/source inputs/binaries, failed/live originals. Restore raw before replay.'}
(out/'duplicate-expansion-recovery.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'recover-duplicate-expansion.py');print(json.dumps(report))
