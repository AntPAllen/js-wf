import sys,json,shutil,subprocess,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/tmp-storage-review-2026-10-07/sixteenth-closed-offload'
out=Path('/tmp/storage-sixteenth-removal-20261007');report=json.loads((out/'removal.json').read_text())
assert 'finished_utc' in report and len(report['removed'])==28
for name in ['removal.json','executed-removal.py']:shutil.copyfile(out/name,base/name)
units=['js-wf-parallel-recovery-journal-24h-20261007.service','js-wf-parallel-recovery-journal-watch-24h-20261007.service','js-wf-parallel-recovery-journal-review-24h-20261007.service']
live={unit:subprocess.check_output(['systemctl','is-active',unit],text=True).strip() for unit in units}
assert all(s=='active' for s in live.values())
size=subprocess.run(['du','-x','-B1','--max-depth=1','/tmp'],text=True,capture_output=True)
assert size.returncode in (0,1)
rows=[]
for line in size.stdout.splitlines():
 n,p=line.split('\t',1);rows.append({'path':p,'allocated_bytes':int(n)})
total=next(x['allocated_bytes'] for x in rows if x['path']=='/tmp')
final={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'live_units':live,'tmp_visible_allocated_bytes':total,'free_bytes':shutil.disk_usage('/tmp').free,'size_scan_errors':size.stderr,'largest_remaining':sorted([x for x in rows if x['path']!='/tmp'],key=lambda x:x['allocated_bytes'],reverse=True)[:15],'reclaimed_original_allocated_bytes':report['reclaimed_original_allocated_bytes'],'removed_staging_archive_allocated_bytes':report['removed_staging_archive_allocated_bytes']}
(base/'final-storage.json').write_text(json.dumps(final,indent=2)+'\n')
p=base/'README.md';text=p.read_text();text=text.replace('Retirement totals and final live-run checks will be recorded after cleanup.',f"Completed: **{report['reclaimed_original_allocated_bytes']/2**30:.2f} GiB** of pre-existing allocated storage reclaimed from all 28 selected directories, plus **{report['removed_staging_archive_allocated_bytes']/2**30:.2f} GiB** of newly created archive staging removed. `/tmp` now uses approximately **{total/2**30:.2f} GiB**, with **{final['free_bytes']/2**30:.2f} GiB** free on the filesystem. All three original 24-hour test, observer and reviewer units remain active. See the [exact removal ledger](removal.json) and [final storage snapshot](final-storage.json).")
p.write_text(text)
shutil.copyfile(__file__,base/'executed-final-snapshot.py')
print(json.dumps(final,indent=2))
