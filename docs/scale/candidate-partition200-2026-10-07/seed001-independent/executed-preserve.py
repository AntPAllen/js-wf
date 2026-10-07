import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,subprocess,shutil,datetime
sys.path.insert(0,'/tmp');from storage_review_common import repo,closure,fixture_archive
campaign=Path('/tmp/js-wf-candidate-partition200-v2-20261007/campaign');root=campaign/'seed-001';review=Path('/tmp/js-wf-candidate-partition200-seed001-independent-20261007');out=repo/'docs/scale/candidate-partition200-2026-10-07/seed001-independent';out.mkdir()
state=json.loads((campaign/'campaign.json').read_text());record=state['records'][0]
assert record['seed']==1 and record['exit_code']==0 and record['profile_verified'] is True
result=json.loads((review/'seed-001.json').read_text());assert result['native_seed_qualified'] is True and result['server_profile']=='experimental-component-candidate'
assert json.loads((review/'review.json').read_text())['qualifies_full_row'] is False
before=closure(root);model_closed=closure(review)
for name in ['execution.json','acceptance.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','native.log','acceptance.log','partition-server-input.json','partition-server-verification.json','commands.json']:
 shutil.copyfile(root/name,out/name)
for name in ['seed-001.json','review.json','candidate-proof-controls.json']:shutil.copyfile(review/name,out/name)
shutil.copyfile(repo/'scripts/review-local-tier2-partition.py',out/'executed-review.py');shutil.copyfile(repo/'scripts/check-partition-candidate-proof-controls.py',out/'executed-proof-controls.py');shutil.copyfile(__file__,out/'executed-preserve.py')
(out/'campaign-record.json').write_text(json.dumps(record,indent=2)+'\n')
(out/'closure.json').write_text(json.dumps({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native':before,'reviewer':model_closed},indent=2)+'\n')
fixture_archive.capture(root,Path('/tmp/js-wf-candidate-partition-seed001-complete-20261007.tar.gz'),out,compresslevel=1)
model=out/'reviewer-artifacts';model.mkdir()
fixture_archive.capture(review,Path('/tmp/js-wf-candidate-partition-seed001-reviewer-20261007.tar.gz'),model,compresslevel=1)
print('CANDIDATE_SEED001_COMPLETE_NATIVE_AND_REVIEWER_CAPTURED',flush=True)
