from storage_review_common import *
root=Path('/tmp/js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005')
out=Path('/tmp/scale-donor-capture-20261007'); out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-capture.py')
report=dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),root=str(root),closure_before=closure(root),original_execution=json.loads((root/'execution.json').read_text()))
assert report['original_execution']['status']=='failed' and report['original_execution']['exit_code']==1
(out/'capture-review.json').write_text(json.dumps(report,indent=2)+'\n')
print('CAPTURE_CLOSED_DONOR',flush=True)
meta=fixture_archive.capture(root,Path(str(root)+'.tar.gz'),out,compresslevel=1)
report.update(closure_after=closure(root),archive=meta,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out/'capture-review.json').write_text(json.dumps(report,indent=2)+'\n')
print('CAPTURE_VERIFIED',meta['archive_bytes'],flush=True)
