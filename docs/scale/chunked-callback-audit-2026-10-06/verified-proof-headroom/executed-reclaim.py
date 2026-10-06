from pathlib import Path,PurePosixPath
import json,hashlib,subprocess,os,shutil,datetime,sys
repo=Path('/home/exedev/js-wf');inventory=json.loads(Path('/tmp/js-wf-closed-chunked-proof-candidates-20261006.json').read_text())
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head==inventory['head']
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
out=repo/'docs/scale/chunked-callback-audit-2026-10-06/verified-proof-headroom';out.mkdir()
(out/'executed-reclaim.py').write_text(Path(__file__).read_text())
report={'head':head,'pushed_main_matches':True,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'target_free_bytes':5*1024**3,'scope':'Only exact duplicate combined temporary proof archives; all original stores, original provider media/zips, source/binaries, live campaign, build caches and canonical Git parts retained','removed':[],'skipped':[]}
def save():
 report['removed_bytes']=sum(r['bytes'] for r in report['removed']);report['free_bytes']=shutil.disk_usage(repo).free
 (out/'reclamation.json').write_text(json.dumps(report,indent=2)+'\n')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def nofds(path):
 for proc in Path('/proc').glob('[0-9]*'):
  for task in (proc/'task').glob('[0-9]*'):
   try:fds=list((task/'fd').iterdir())
   except (OSError,PermissionError):continue
   for fd in fds:
    try:target=os.readlink(fd)
    except (OSError,PermissionError):continue
    assert target!=str(path),('open archive',str(fd),target)
save()
for candidate in inventory['candidates']:
 if shutil.disk_usage(repo).free>=report['target_free_bytes']:break
 path=Path(candidate['path']);assert path.name in ('proof.tar.gz','proof-delta.tar.gz') and path.parent.parent==Path('/tmp')
 closure=candidate['closure']
 if closure['kind']=='local-sdk':
  assert not Path('/proc/'+str(closure['pid'])).exists()
  e=json.loads((path.parent/'execution.json').read_text());assert e==closure['execution']
 else:
  kind='jobs' if closure['kind']=='github-job' else 'runs';cid=closure['id']
  actual=json.loads(subprocess.check_output(['gh','api',f'repos/AntPAllen/js-wf/actions/{kind}/{cid}'],text=True))
  if actual['status']!='completed' or actual['conclusion']!=closure['conclusion']:
   report['skipped'].append({'path':str(path),'reason':'primary API terminal state no longer matches'});save();continue
  (out/(closure['kind']+'-'+str(cid)+'.json')).write_text(json.dumps(actual,indent=2)+'\n')
  closure={**closure,'primary_api_terminal_state_verified':True,'completed_at':actual.get('completed_at',actual.get('updated_at'))}
 metaName=candidate['canonical_metadata'];meta=json.loads(subprocess.check_output(['git','show',head+':'+metaName],cwd=repo))
 supported=meta.get('all_archive_members_and_parts_read_back') or (meta.get('all_original_member_hashes_verified') and meta.get('parts_and_concat_sha_verified')) or (meta.get('all_members_readback_sha_verified') and meta.get('concatenated_parts_readback_sha_verified')) or (meta.get('all_members_readback_verified') and (meta.get('all_parts_readback_verified') or meta.get('parts_readback_sha_verified')))
 assert supported and meta['archive_sha256']==candidate['sha256'] and meta['archive_bytes']==candidate['bytes']==path.stat().st_size
 digest=hashlib.sha256();total=0
 for part in meta['parts']:
  name=part.get('file',part.get('name',part.get('path')));assert PurePosixPath(name).name==name
  b=subprocess.check_output(['git','show',head+':'+str(Path(metaName).parent/name)],cwd=repo)
  assert len(b)==part['bytes'] and hashlib.sha256(b).hexdigest()==part['sha256']
  digest.update(b);total+=len(b)
 assert digest.hexdigest()==candidate['sha256'] and total==candidate['bytes'] and sha(path)==candidate['sha256']
 if path.name=='proof-delta.tar.gz':
  sys.path.insert(0,str(repo/'scripts'));import fixture_delta
  assert (repo/'scripts/fixture_delta.py').read_bytes()==subprocess.check_output(['git','show',head+':scripts/fixture_delta.py'],cwd=repo)
  assert fixture_delta.verify(path,repo)['complete_virtual_tree_verified']
 nofds(path)
 assert sha(path)==candidate['sha256']
 record={'path':str(path),'bytes':candidate['bytes'],'sha256':candidate['sha256'],'canonical_metadata':metaName,'all_pushed_part_and_concat_hashes_verified':True,'all_visible_task_fds_checked':True,'closure':closure}
 path.unlink();report['removed'].append(record);save()
 print('RECLAIMED',record['bytes'],path.parent.name,'free',report['free_bytes'],flush=True)
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['removed_bytes'],'free',report['free_bytes'],flush=True)
