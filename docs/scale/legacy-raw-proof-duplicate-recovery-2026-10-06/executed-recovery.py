from pathlib import Path
import hashlib,io,json,subprocess,sys,tarfile,importlib.util,shutil,datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
blob=lambda path:subprocess.check_output(['git','cat-file','blob',head+':'+path],cwd=repo)
prior=json.loads(blob('docs/scale/legacy-proof-parts-recovery-2026-10-06/recovery.json'))
tracked=set(subprocess.check_output(['git','ls-files'],cwd=repo,text=True).splitlines())
candidates={}
for root in Path('/tmp').glob('js-wf-*'):
 if root.is_dir():
  for name in ['proof.tar.gz','originals.tar.gz','compact-proof.tar.gz','provider-proof.tar.gz']:
   p=root/name
   if p.is_file() and not p.is_symlink():candidates.setdefault(p.stat().st_size,[]).append(p)
records=[]
for prior_group in prior['verified_archives']:
 group=prior_group['group'];matches=[]
 for p in candidates.get(prior_group['archive_bytes'],[]):
  if closed.sha(p)==prior_group['archive_sha256']:matches.append(p)
 if not matches:continue
 meta=None
 for name in ['manifest.json','parts.json','archive-verification.json']:
  if group+'/'+name in tracked:
   candidate=json.loads(blob(group+'/'+name))
   if 'parts' in candidate or 'original_parts' in candidate:meta=candidate;break
 assert meta is not None
 parts=meta.get('parts',meta.get('original_parts'));chunks=[];combined=hashlib.sha256();size=0
 for part in parts:
  name=part.get('path',part.get('file',part.get('name')));fixture_delta.safe_name(name);data=blob(group+'/'+name)
  assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'];chunks.append(data);combined.update(data);size+=len(data)
 assert combined.hexdigest()==prior_group['archive_sha256'] and size==prior_group['archive_bytes']
 actual={};embedded=None;hardlinks=0
 with tarfile.open(fileobj=io.BytesIO(b''.join(chunks)),mode='r|gz') as archive:
  for member in archive:
   fixture_delta.safe_name(member.name)
   if member.isdir():continue
   assert member.name not in actual
   if member.islnk():
    fixture_delta.safe_name(member.linkname);assert member.linkname in actual;actual[member.name]=actual[member.linkname];hardlinks+=1
   else:
    assert member.isfile()
    if member.name=='archive-manifest.json' and meta.get('all_archive_members_and_parts_read_back'):
     assert embedded is None;embedded=json.load(archive.extractfile(member))
    else:actual[member.name]=fixture_delta.digest(archive.extractfile(member))
 del chunks
 if embedded is not None:expected=embedded
 elif 'files' in meta:expected=meta['files']
 else:
  for name in ['archive-manifest.json','original-member-manifest.json']:
   if group+'/'+name in tracked:expected=json.loads(blob(group+'/'+name))['files'];break
  else:raise ValueError('Missing member inventory')
 assert set(actual)==set(expected)
 for name,value in expected.items():
  if isinstance(value,str):assert actual[name]['sha256']==value
  else:assert actual[name]=={k:value[k] for k in ['bytes','sha256']}
 assert len(actual)==prior_group['members'] and hardlinks==prior_group['hardlinks']
 for raw in matches:
  root=raw.parent;live=[];limits=[]
  for proc in Path('/proc').glob('[0-9]*'):
   try:
    exe=(proc/'exe').resolve();args=(proc/'cmdline').read_bytes().split(b'\0')
    if exe.is_relative_to(root) or (args and args[0].decode(errors='replace').startswith(str(root)+'/')):live.append(int(proc.name))
   except PermissionError:limits.append(int(proc.name))
   except (FileNotFoundError,ProcessLookupError):pass
  assert not live,(root,live)
  ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
  for c in running:
   for mount in c['Mounts']:
    source=Path(mount['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
  fd=closed.verify_no_open_originals(raw)
  records.append(dict(path=str(raw),canonical=group,sha256=prior_group['archive_sha256'],bytes=raw.stat().st_size,allocated_bytes=raw.stat().st_blocks*512,full_canonical_parts_concat_and_member_inventory_verified=True,members=len(actual),hardlinks=hardlinks,no_visible_root_process_or_running_docker_mount=True,unobservable_processes=limits,visible_fd_check=fd))
 print('VERIFIED',group,len(matches),flush=True)
assert records and subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/legacy-raw-proof-duplicate-recovery-2026-10-06';out.mkdir();shutil.copyfile(__file__,out/'executed-recovery.py')
for r in records:
 p=Path(r['path']);assert closed.sha(p)==r['sha256'];p.unlink()
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),duplicates=records,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only hash-identical raw archive duplicates removed after complete canonical Git parts/concat/member/hardlink verification. Primary original stores/source/exes/metadata/caches and canonical Git proof retained. No NATS/SDK startup or verdict changes. Process/descriptor visibility limits recorded.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
