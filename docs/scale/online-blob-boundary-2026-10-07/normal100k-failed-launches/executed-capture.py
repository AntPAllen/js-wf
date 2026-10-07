from storage_review_common import *
root=Path('/tmp/js-wf-online-blob-normal100k-20261007');out=Path('/tmp/js-wf-online-blob-failed-launches-proof-20261007')
out.mkdir(exist_ok=False);shutil.copyfile(__file__,out/'executed-capture.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
for name in ('prelaunch-failure','admission-failure'):
 shutil.copyfile(Path('/tmp/js-wf-online-blob-normal100k-'+name+'-20261007.log'),out/(name+'.log'))
unit=subprocess.check_output(['systemctl','show','js-wf-online-blob-normal100k-admitted-20261007.service','-p','MainPID','-p','SubState','-p','ExecMainStatus'],text=True)
assert 'MainPID=0\n' in unit and 'SubState=failed\n' in unit and 'ExecMainStatus=1\n' in unit
before=closure(root)
assert 'completed=0 requested=100000' in (root/'native.log').read_text() and '--- FAIL: TestSeededOnlineBlobBoundaryReplay' in (root/'native.log').read_text()
meta=fixture_archive.capture(root,Path('/tmp/js-wf-online-blob-failed-launches-20261007.tar.gz'),out,compresslevel=1)
report=dict(original_unit=unit,qualification=False,actual_sdk_not_admitted=True,original_failure='Sample trace directory missing; actual test failed before first completed body. First separate root-owned launch stopped before build on Git ownership check.',scope='Complete unchanged failed root archived; no rerun or verdict repair.',closure_before=before,closure_after=closure(root),archive=meta)
(out/'capture-review.json').write_text(json.dumps(report,indent=2)+'\n')
print('FAILED_LAUNCHES_PRESERVED',meta['archive_bytes'],flush=True)
