import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,shutil,datetime,hashlib,subprocess
sys.path.insert(0,'/tmp');from storage_review_common import repo,closure,fixture_archive
campaign=Path('/tmp/js-wf-candidate-partition200-v2-20261007/campaign')
state=json.loads((campaign/'campaign.json').read_text())
for seed in [2,3]:
 tag=f'{seed:03d}';root=campaign/f'seed-{tag}';review=Path(f'/tmp/js-wf-candidate-partition200-seed{tag}-independent-20261007');out=repo/f'docs/scale/candidate-partition200-2026-10-07/seed{tag}-independent';out.mkdir()
 record=next(r for r in state['records'] if r['seed']==seed)
 assert record['exit_code']==0 and record['profile_verified'] is True
 result=json.loads((review/f'seed-{tag}.json').read_text());assert result['native_seed_qualified'] is True and result['server_profile']=='experimental-component-candidate'
 assert json.loads((review/'review.json').read_text())['qualifies_full_row'] is False
 before=closure(root);model_closed=closure(review)
 for name in ['execution.json','acceptance.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','native.log','acceptance.log','partition-server-input.json','partition-server-verification.json','commands.json']:
  shutil.copyfile(root/name,out/name)
 for name in [f'seed-{tag}.json','review.json']:shutil.copyfile(review/name,out/name)
 for name in ['review-local-tier2-partition.py','check-tier2-journal-shard.py','verify-tier2-closed-originals.py','fixture_archive.py','fixture_delta.py']:
  shutil.copyfile(repo/'scripts'/name,out/('executed-'+name))
 shutil.copyfile(__file__,out/'executed-preserve.py')
 (out/'campaign-record.json').write_text(json.dumps(record,indent=2)+'\n')
 (out/'closure.json').write_text(json.dumps({'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native':before,'reviewer':model_closed},indent=2)+'\n')
 native=fixture_archive.capture(root,Path(f'/tmp/js-wf-candidate-partition-seed{tag}-complete-20261007.tar.gz'),out,compresslevel=1)
 model=out/'reviewer-artifacts';model.mkdir()
 artifacts=fixture_archive.capture(review,Path(f'/tmp/js-wf-candidate-partition-seed{tag}-reviewer-20261007.tar.gz'),model,compresslevel=1)
 (out/'README.md').write_text(f'''# Candidate partition seed {seed} independent review

Original native source `{state['source']}`. The actual nonrace SDK passes the original ten-minute partition workload and is independently accepted against all nineteen raw cuts, latency samples, three history models, captured source/dependency inputs and three original candidate server identities/bytes. Native source, deadlines, seed and production TTL/marker requirements are unchanged.

The report preserves the producer's final integrity/drain assertions and complete closed-store inventory. It does not independently decode the physical stores or start any broker. This is one experimental candidate seed, not the complete 200-seed row or default-server acceptance. Candidate proof substitution controls previously rejected all 27 mutations on seed 1.

Native complete archive: {native['archive_bytes']:,} bytes, SHA256 `{native['archive_sha256']}`. Reviewer complete archive: {artifacts['archive_bytes']:,} bytes, SHA256 `{artifacts['archive_sha256']}`. Complete member inventories are retained here; S3 receipts accompany successful upload/full byte readback. Raw native roots remain local for the running campaign.
''')
 print('CAPTURED',seed,native['archive_bytes'],artifacts['archive_bytes'],flush=True)
