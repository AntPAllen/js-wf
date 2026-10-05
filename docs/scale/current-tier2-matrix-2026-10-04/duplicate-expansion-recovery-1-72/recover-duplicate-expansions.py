import pathlib,json,hashlib,subprocess,tarfile,zipfile,os,shutil
repo=pathlib.Path('/home/exedev/js-wf');out=repo/'docs/scale/current-tier2-matrix-2026-10-04/duplicate-expansion-recovery-1-72';out.mkdir(exist_ok=True)
remote=subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0];assert remote==subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
def digest(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
reports=json.loads((out/'recovery.json').read_text())['ranges']
completed={x['range'] for x in reports}
for first in range(1,73,12):
 label=f'{first}-{first+11}'
 if label in completed:continue
 root=pathlib.Path(f'/tmp/js-wf-tier2-current-cluster{label}-37149506857');published=repo/f'docs/scale/current-tier2-matrix-2026-10-04/cluster-{label}';manifest=json.loads((published/'manifest.json').read_text());summary=json.loads((published/'summary.json').read_text());assert summary['all_three_independent_history_models_pass']
 h=hashlib.sha256()
 for part in manifest['parts']:
  p=published/part['path'];data=p.read_bytes();assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'];assert subprocess.check_output(['git','show',remote+':'+str(p.relative_to(repo))],cwd=repo)==data;h.update(data)
 assert h.hexdigest()==manifest['archive_sha256']==digest(root/'proof.tar.gz')
 with tarfile.open(root/'proof.tar.gz',mode='r|gz') as t:
  seen=set()
  for member in t:
   if member.name not in manifest['files']:continue
   assert member.name not in seen;seen.add(member.name)
   entry=manifest['files'][member.name];f=t.extractfile(member)
   assert f is not None and hashlib.file_digest(f,'sha256').hexdigest()==entry['sha256']
  assert seen==set(manifest['files'])
 with zipfile.ZipFile(root/'raw.zip') as z:
  assert len(z.infolist())==72
  for info in z.infolist():
   with z.open(info) as f:assert hashlib.file_digest(f,'sha256').hexdigest()==manifest['files']['artifact/'+info.filename]['sha256']
 raw=root/'raw';count=0;allocated=0
 for p in raw.rglob('*'):
  if p.is_file():
   assert digest(p)==manifest['files']['artifact/'+str(p.relative_to(raw))]['sha256'];count+=1;allocated+=p.stat().st_blocks*512
 assert count==72
 targets=[]
 for proc in pathlib.Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   for fd in (proc/'fd').iterdir():
    try:target=os.readlink(fd)
    except OSError:continue
    if target==str(raw) or target.startswith(str(raw)+'/'):targets.append(str(fd))
  except OSError:pass
 assert not targets
 shutil.rmtree(raw)
 report={'range':label,'published_commit':remote,'raw_files_verified':count,'exclusive_allocated_bytes_recovered':allocated,'canonical_archive_zip_all_members_and_published_parts_verified':True,'visible_target_descriptors':targets,'removed_only_duplicate_raw_expansion':True,'retained':'Original ZIP, canonical proof, model/source inputs/binaries; failed/live originals untouched. Restore raw before replay.'}
 reports.append(report);(out/'recovery.json').write_text(json.dumps({'ranges':reports,'total_allocated_bytes_recovered':sum(x['exclusive_allocated_bytes_recovered'] for x in reports)},indent=2)+'\n');print(label,allocated,flush=True)
shutil.copyfile(__file__,out/'recover-duplicate-expansions.py')
