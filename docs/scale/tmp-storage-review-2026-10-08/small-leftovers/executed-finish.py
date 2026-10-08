import sys,json,subprocess,hashlib,shutil,datetime
from pathlib import Path
base=Path('/home/exedev/js-wf/docs/scale/tmp-storage-review-2026-10-08/small-leftovers')
report=json.loads((base/'removal-pending.json').read_text());c=json.loads((base/'capture.json').read_text());meta=json.loads((base/'archive-verification.json').read_text());archive=Path(c['archive'])
assert all(not Path(p).exists() for p in c['roots'])
assert archive.stat().st_size==meta['archive_bytes'] and hashlib.sha256(archive.read_bytes()).hexdigest()==meta['archive_sha256']
closure=json.loads(subprocess.check_output(['sudo','-n','python3','/tmp/tmp-small-leftovers-20261008.py','closure'],text=True));assert not closure['blocked'] and not closure['permission_limits']
archive.unlink()
report.update(free_after=shutil.disk_usage('/tmp').free,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),all_selected_paths_absent=True,finish_closure=closure,completion_note='Initial removal deleted all nine directories; archive unlink hit root ownership in sticky /tmp. Ownership corrected, archive hash and closure rechecked, then archive removed.')
(base/'removal.json').write_text(json.dumps(report,indent=2)+'\n');(base/'removal-pending.json').unlink();shutil.copyfile(__file__,base/'executed-finish.py')
with (base/'README.md').open('a') as f:f.write('\nAll nine selected directories and the transfer archive were removed after committed/pushed receipts, fresh complete remote archive/member verification, unchanged originals, and privileged process/descriptor/container/mount/loop checks. S3 is the canonical preserved copy.\n')
print(report['reclaimed_allocated_bytes_including_archive'])
