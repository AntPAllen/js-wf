from pathlib import Path
import sys, subprocess, json, hashlib, shutil, importlib.util, datetime
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_delta
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def census(root):
 result={}
 for p in root.rglob('*'):
  assert not p.is_symlink()
  if p.is_file():
   assert p.stat().st_nlink==1
   result[p.relative_to(root).as_posix()]=dict(bytes=p.stat().st_size,sha256=closed.sha(p))
 return result
def closure(root):
 limits=[]
 for p in Path('/proc').glob('[0-9]*'):
  try:
   exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
   assert not exe.is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in args),(p,args)
  except PermissionError:limits.append(str(p))
  except (FileNotFoundError,ProcessLookupError):pass
 ids=subprocess.check_output(['docker','ps','-q'],text=True).split()
 running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 for c in running:
  for m in c['Mounts']:
   source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
 loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
 for device in loops['loopdevices']:
  assert not Path(device['back-file']).resolve().is_relative_to(root)
 mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
 def walk(rows):
  for row in rows:
   assert not Path(row['target']).resolve().is_relative_to(root)
   assert not row['source'].startswith(str(root))
   walk(row.get('children',[]))
 walk(mounts['filesystems'])
 return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),loopdevices=loops,mounts=mounts,running_docker_ids=ids)
records=[]
pairs=[]
for phase in ('create','results'):
 for position in ('first','interior','last'):
  case=phase+'-'+position
  pairs.append(('/tmp/js-wf-fanout-local-physical-copy-'+case+'-20261006','docs/scale/fanout-combined-local-physical-2026-10-06/copied-local-physical/'+case))
  for category,group in [('success','copied-audits'),('drained','drained-copied-audits')]:
   pairs.append(('/tmp/js-wf-fanout-'+category+'-copied-audit-'+case+'-20261005-v2','docs/scale/fanout-combined-boundaries-2026-10-05/'+group+'/'+phase+'/'+position))
for rootname,group in pairs:
 root=Path(rootname);copy=root/'cluster'
 canonical=group+'/archive-verification.json'
 expected=fixture_delta.read_base(repo,head,canonical)
 assert expected==json.loads((root/'archive-manifest.json').read_text())
 before=census(root)
 assert before==dict(expected,**{'archive-manifest.json':dict(bytes=(root/'archive-manifest.json').stat().st_size,sha256=closed.sha(root/'archive-manifest.json'))})
 provenance=json.loads((root/'copy-before.json').read_text())
 assert provenance['all_initial_copy_bytes_match'] is True
 assert Path(provenance['original_root']).resolve()!=copy.resolve()
 detached=json.loads((root/'copy-after.json').read_text())
 copied={k[len('cluster/'):]:v for k,v in expected.items() if k.startswith('cluster/')}
 if 'files' in detached:assert copied==detached['files']
 else:assert {k:v['sha256'] for k,v in copied.items()}==detached['closed_copy_files']
 e=json.loads((root/'execution.json').read_text())
 assert e['status']=='passed' and e['exit_code']==0 and e['native_R3_servers_embedded_in_sdk'] is True and not Path('/proc',str(e['pid'])).exists()
 assert closed.sha(root/'review-sdk')==e['actual_sha256']
 visible=closure(root)
 allocated=sum(p.stat().st_blocks*512 for p in copy.rglob('*') if p.is_file())
 records.append(dict(root=str(root),copy=str(copy),canonical=canonical,archive_members=len(expected),copied_files=len(copied),sdk_pid=e['pid'],R3_servers_embedded_in_closed_sdk=True,complete_canonical_parts_concat_embedded_manifest_and_current_census_verified=True,copied_origin_and_closed_file_ledgers_verified=True,closure=visible,allocated_bytes=allocated,before=before))
 print('VERIFIED',root,len(copied),allocated,flush=True)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==head
out=repo/'docs/scale/closed-fanout-copy-recovery-2026-10-06';out.mkdir();shutil.copyfile(__file__,out/'executed-recovery.py')
for record in records:
 root=Path(record['root']);copy=Path(record['copy']);assert census(root)==record['before'];closure(root)
 shutil.rmtree(copy)
 assert census(root)=={k:v for k,v in record.pop('before').items() if not k.startswith('cluster/')}
 record['all_remaining_source_executable_metadata_files_unchanged']=True
report=dict(head=head,pushed_main_matches=True,observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),copies=records,allocated_bytes_recovered=sum(r['allocated_bytes'] for r in records),free_bytes=shutil.disk_usage(repo).free,scope='Only closed disposable copied fanout store trees removed after complete committed archive/current census, SDK/embedded-R3-server executable closure, loop/mount/Docker and visible descriptor checks. Original donor media, source/executables/metadata, caches and canonical Git archive parts retained. No NATS startup or verdict change. Visibility limitations recorded.')
(out/'recovery.json').write_text(json.dumps(report,indent=2)+'\n');print('RECOVERED',report['allocated_bytes_recovered'],'FREE',report['free_bytes'],flush=True)
