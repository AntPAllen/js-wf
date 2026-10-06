from pathlib import Path
import json,subprocess,datetime
repo=Path('/home/exedev/js-wf')
b=repo/'docs/scale/tmp-storage-review-2026-10-06/further-closed-roots'
reports=[json.loads(p.read_text()) for p in b.glob('*/offload.json')]
assert len(reports)==11
free=__import__('shutil').disk_usage('/tmp').free
summary=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),closed_roots_offloaded=len(reports),preexisting_allocated_bytes_reclaimed=sum(r['allocated_bytes_removed'] for r in reports),new_archive_staging_bytes_removed=sum(r['staging_bytes_removed'] for r in reports),available_bytes=free,scope='Net reclamation excludes newly created staging archives. Running campaigns and capacity donor stores remain local. Original million-timer primary, symlink disk-stall root, attachments, caches and system temporary directories untouched.',live_units={unit:subprocess.check_output(['systemctl','--user','show',unit,'-p','ActiveState','-p','MainPID'],text=True) for unit in ['js-wf-bulk-journal-24h-joined-20261006.service','js-wf-local-partition200-20261006.service']})
(b/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
