from pathlib import Path
import sys,subprocess,json,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
base=repo/'docs/scale/local-tier2-partition-2026-10-06';original=Path('/tmp/js-wf-local-partition200-20261006/seed-001');review=Path('/tmp/js-wf-local-partition200-seed1-review-20261006');controls=Path('/tmp/js-wf-local-partition200-raw-controls-20261006')
def closure(root):
 limits=[]
 for proc in Path('/proc').glob('[0-9]*'):
  try:
   assert not (proc/'exe').resolve().is_relative_to(root)
   for link in ('cwd','root'):assert not (proc/link).resolve().is_relative_to(root)
   assert not any(str(root).encode() in arg for arg in (proc/'cmdline').read_bytes().split(b'\0'))
  except PermissionError:limits.append(str(proc))
  except (FileNotFoundError,ProcessLookupError):pass
 return dict(process_inspection_limits=limits,visible_descriptors=closed.verify_no_open_originals(root))
# The review context preserves the exact campaign checkpoint and preparation
# ledgers used to bind this closed seed. Native files remain unchanged.
context=review/'campaign-context';context.mkdir()
campaign=original.parent
state=json.loads((campaign/'campaign.json').read_text());record=state['records'][0];assert record['seed']==1 and record['exit_code']==0
(context/'campaign-checkpoint.json').write_text(json.dumps(state,indent=2)+'\n')
for name in ('executed-campaign.py','prepared/preparation.json','prepared/source-before.json','prepared/source-after.json','prepared/external-source-before.json','prepared/external-source-after.json'):
 target=context/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(campaign/name,target)
for name in ('review-local-tier2-partition.py','check-tier2-journal-shard.py','tier2-history-review.go.txt'):
 shutil.copy2(repo/'scripts'/name,review/name)
for label,root in [('seed-001-native',original),('seed-001-review',review),('seed-001-controls',controls)]:
 out=base/label;initial=closure(root);proof=fixture_archive.capture(root,Path('/tmp')/('js-wf-local-partition-'+label+'-20261006.tar.gz'),out,compresslevel=1);final=closure(root)
 (out/'capture.json').write_text(json.dumps(dict(root=str(root),closure_before=initial,closure_after=final,archive=proof,scope='Complete closed original native seed or independent review/control evidence. No broker opened, original removed or full200 gate promoted.'),indent=2)+'\n')
 print('CAPTURED',label,proof['members'],proof['archive_bytes'],flush=True)
shutil.copy2(__file__,base/'executed-seed1-preservation.py')
shutil.copy2(review/'review.json',base/'seed-001-review-summary.json');shutil.copy2(controls/'controls.json',base/'seed-001-control-summary.json')
