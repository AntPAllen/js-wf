import sys,json,shutil
from pathlib import Path
sys.dont_write_bytecode=True
base=Path('/home/exedev/js-wf/docs/scale/corrected-partition200-2026-10-08/reclaimed')
p=base/'removal.json';original=p.read_bytes();r=json.loads(original)
assert r['reclaimed_allocated_bytes']==1291558912
(base/'original-member-allocation-report.json').write_bytes(original)
r['summed_member_allocated_bytes_including_hardlink_duplicates']=r.pop('reclaimed_allocated_bytes')
r['member_allocated_bytes_by_path_including_hardlink_duplicates']=r.pop('allocated_bytes')
r['filesystem_free_change_bytes']=r['free_bytes_after']-r['free_bytes_before']
r['measurement_correction']='The executed remover summed st_blocks for each member path. Prepared native inputs share hardlinks, so that sum is not physical reclaimed space. Deleted inode/link inventory was not captured; exact per-fixture physical reclaim cannot be recovered from the regular-file archive. Filesystem free-space change is measured directly, with concurrent live SDK output/cache activity.'
r['original_report_file']='original-member-allocation-report.json'
p.write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-accounting-correction.py')
(base/'README.md').write_text('# Failed corrected campaign fixture retired\n\nThe closed original campaign, complete archive and registered clean source worktree were retired after fresh full S3 compressed-byte/member verification, unchanged local inventories, committed/pushed receipts and visible process/descriptor/Docker/mount/loop closure checks. Permission limits are recorded. Failed seed2 and unexecuted seeds3–200 remain unchanged. No native store was reopened or workload retried. Restore the failed original to a fresh directory from its canonical S3 receipt before further store inspection.\n\nFilesystem free space increased951947264bytes during retirement. This is an observed filesystem change with concurrent SDK activity, not exact per-fixture physical reclaim. The original member allocation sum double-counted hardlinked prepared inputs. [Corrected ledger](removal.json) preserves the measurements and limitation; [original executed report](original-member-allocation-report.json) retains the uncorrected sum.\n')
print('CORRECTED_STORAGE_ACCOUNTING',r['filesystem_free_change_bytes'])
