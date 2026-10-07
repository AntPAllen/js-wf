import sys,json,datetime,shutil,subprocess
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,'/tmp')
from storage_review_common import *
root=Path('/tmp/js-wf-candidate-partition200-v2-20261007/campaign')
base=repo/'docs/scale/tmp-storage-review-2026-10-07/twenty-fourth-candidate-closed'
base.mkdir()
shutil.copyfile(__file__,base/'executed-capture.py')
shutil.copyfile('/tmp/storage_review_common.py',base/'executed-common.py')
campaign=json.loads((root/'campaign.json').read_bytes())
assert campaign['exit_code']==1 and campaign['current_seed']==31 and campaign['finished_utc']
unit=subprocess.check_output(['systemctl','--user','show','js-wf-candidate-partition200-v2-20261007.service','-p','ActiveState','-p','Result','-p','ExecMainStatus'],text=True)
assert 'ActiveState=failed' in unit and 'ExecMainStatus=1' in unit
(base/'unit-terminal.txt').write_text(unit)
(base/'closure-before.json').write_text(json.dumps(closure(root),indent=2)+'\n')
(base/'origin.json').write_text(json.dumps(dict(root=str(root),campaign_terminal=campaign,captured_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),qualification='Storage preservation only. Campaign stopped on seed 31 failure. No independent acceptance or cause attribution; no native rerun. Source checkout and external candidate input retained separately.'),indent=2)+'\n')
print('CAPTURING',flush=True)
print(fixture_archive.capture(root,Path('/tmp/candidate-closed-campaign-oct7.tar.gz'),base,compresslevel=1),flush=True)
(base/'closure-after.json').write_text(json.dumps(closure(root),indent=2)+'\n')
(base/'README.md').write_text('# Closed candidate campaign storage\n\nComplete campaign files from seeds 1–31, including failed seed 31, are archived for S3. This storage review does not independently qualify the campaign or attribute its failure. Source checkout and candidate input remain local; restore all archived campaign files to a fresh directory before review or reuse. The active full deterministic simulation is excluded.\n')
