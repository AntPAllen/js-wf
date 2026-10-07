from storage_review_common import *
base=repo/'docs/scale/tmp-storage-review-2026-10-07/eighteenth-closed-fixtures'
base.mkdir(exist_ok=False)
shutil.copyfile(__file__,base/'executed-capture.py')
shutil.copyfile('/tmp/storage_review_common.py',base/'executed-common.py')
names=['js-wf-outer-handler-lease-expiry-race-20261006','js-wf-outer-handler-lease-expiry-race-corrected-20261006','js-wf-million-direct-filestore-20261002','js-wf-million-store-copy-20261002','js-wf-million-rebuilt-target-review-20261002']
worktrees=[Path(x[9:]) for x in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if x.startswith('worktree ')]
for name in names:
 root=Path('/tmp')/name; out=base/name
 assert not any(w.exists() and (w.is_relative_to(root) or root.is_relative_to(w)) for w in worktrees),'existing registered worktree'
 before=closure(root)
 proof=fixture_archive.capture(root,Path('/tmp')/(name+'-storage-oct7.tar.gz'),out,compresslevel=1)
 after=closure(root)
 (out/'closure.json').write_text(json.dumps(dict(before=before,after=after,scope='Complete closed fixture preservation; original successful, failed and diagnostic verdicts unchanged. No native rerun or store reopening.'),indent=2)+'\n')
 print(name,proof['archive_bytes'],proof['members'],flush=True)
