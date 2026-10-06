from pathlib import Path
import sys,subprocess,json,hashlib,shutil,importlib.util,datetime,os
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py');s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
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
root=Path('/tmp/js-wf-bulk-restart-ten-minute-joined-20261006')
out=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-restart-ten-minute'
units={}
for name,status in [('js-wf-bulk-restart-ten-minute-joined-20261006.service',1),('js-wf-watch-bulk-restart-ten-minute-joined-20261006.service',0),('js-wf-review-bulk-restart-ten-minute-joined-20261006.service',1),('js-wf-review-bulk-restart-terminal-failure-20261006.service',0)]:
 unit=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','--user','show',name,'-p','ActiveState','-p','MainPID','-p','ExecMainStatus'],text=True).splitlines())
 assert unit==dict(ActiveState='failed' if status else 'inactive',MainPID='0',ExecMainStatus=str(status)),(name,unit);units[name]=unit
meta=json.loads((out/'archive-verification.json').read_text());manifest=json.loads((out/'fixture-inventory.json').read_text())
assert fixture_archive.inventory(root)==manifest['files']
raw=Path('/tmp/js-wf-bulk-restart-ten-minute-joined-complete-20261006.tar.gz')
with raw.open('rb') as stream:actual,compressed=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert actual==manifest
review=json.loads((out/'independent-review.json').read_text());assert review['execution']['status']=='failed' and review['native_terminal']['Action']=='fail' and review['failure_before_bulk_stage'] and review['bulk_latency'] is None
assert not (root/'fixture/bulk-latency-audit.json').exists()
corrected=dict(review);corrected['scope']='Failed original10m R5 all-server SIGKILL/restart at e09442a; original30s client retry exhausted with 503 before final bulk audit. No bulk/point comparison, full final integrity or row/default/fullmatrix/24h qualification. Complete original/partial/observer files retained, source/SDK/server closure verified, no stores reopened.'
corrected['review_scope_correction']='Original reused review free-text mentioned point comparison despite explicit failed/missing-stage fields. Original report bytes retained in complete archive; this corrected scope makes no comparison or acceptance claim.'
(out/'corrected-independent-review.json').write_text(json.dumps(corrected,indent=2)+'\n')
(out/'first-review-service.log').write_text(subprocess.check_output(['journalctl','--user','-u','js-wf-review-bulk-restart-ten-minute-joined-20261006.service','--no-pager'],text=True))
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),units=units,current_complete_census_equals_canonical=True,complete_compressed_archive_and_every_member_equal=compressed,closure=closure(root),scope='Independent supplemental complete census/compressed body/member/global visible process/descriptor/mount closure for failed original10m restart. First reviewer missing bulk artifact failure retained; corrected failure reviewer preserves absence. Native failed verdict and original deadlines unchanged.')
(out/'closure-supplement.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-closure-review.py');print('FAILED_RESTART_COMPLETE_PROOF_AND_GLOBAL_VISIBLE_CLOSURE_VERIFIED',flush=True)
