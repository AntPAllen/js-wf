from pathlib import Path
import json,hashlib,shutil
r=Path('/home/exedev/js-wf/docs/scale/current-tier3-block-stall-2026-10-05');old=json.load(open(r/'seeds-1-65-and-79-156-summary.json'));shards=old['shards'][:]
for first,last in [(157,169)]:
 root=Path(f'/tmp/js-wf-tier3-block-stall{first}-{last}-37164231641');out=r/f'seeds-{first}-{last}';s=json.load(open(out/'summary.json'));m=json.load(open(out/'manifest.json'));rows=json.load(open(root/'row-review.json'));models=json.load(open(root/'model-review.json'))
 assert s['source']==old['source'] and s['first']==first and s['last']==last and s['all_three_models_exact_ok'] and m['all_members_readback_verified'] and m['all_parts_readback_verified'] and not m['visible_open_fds']
 assert models['review_helper_git_sha256']==hashlib.sha256((root/'history-review.go').read_bytes()).hexdigest()
 s['completed_cohort_audits']=sum(x['checkpoint_audit_checks']['completed_cohort_audits'] for x in rows['reports'])
 s['review_helper_git_source']=models['review_helper_git_source'];s['review_helper_git_sha256']=models['review_helper_git_sha256'];s['archive_sha256']=m['archive_sha256'];s['archive_members']=len(m['files']);s['archive_parts']=len(m['parts'])
 (out/'summary.json').write_text(json.dumps(s,indent=2)+'\n');shards.append(s)
 for n in ['executed-batch-review.py','executed-model-review.py','preserve-proof.py']:shutil.copy2(root/n,out/n)
seeds=[i for s in shards for i in range(s['first'],s['last']+1)];assert len(seeds)==len(set(seeds))==156 and seeds==list(range(1,66))+list(range(79,170))
summary={'source':old['source'],'qualified_seed_ranges':[[1,65],[79,169]],'qualified_seeds':len(seeds),'failed_unqualified_range':[66,78],'remaining_unqualified_range':[170,200],'shards':shards,'worst_terminal_p99_seconds':max(s['worst_terminal_p99_seconds'] for s in shards),'worst_progress_p99_seconds':max(s['worst_progress_p99_seconds'] for s in shards),'qualifies_full_row':False,'qualifies_final_source':False,'qualifies_full_matrix':False,'qualifies_24h':False}
for k in ['invocations','journal_entries','faults','model_operations','completed_cohort_audits']:summary[k]=sum(s[k] for s in shards)
(r/'seeds-1-65-and-79-169-summary.json').write_text(json.dumps(summary,indent=2)+'\n');shutil.copy2(__file__,r/'executed-156-seed-summary-review.py');print(json.dumps({k:v for k,v in summary.items() if k!='shards'}))
p=r/'README.md';s=p.read_text().replace('Seeds1–65 and79–156 (143 total)','Seeds1–65 and79–169 (156 total)').replace('seeds-1-65-and-79-156-summary.json','seeds-1-65-and-79-169-summary.json').replace('seeds157–200 remain unqualified','seeds170–200 remain unqualified');s+='\n\nLatest extension157–169 adds one independently reviewed complete shard proof,\nwith original fault/latency/cohort checks and all three rebuilt history models.\nEarlier143-seed ledger remains preserved. Failed66–78 stay unqualified.\n';p.write_text(s)
