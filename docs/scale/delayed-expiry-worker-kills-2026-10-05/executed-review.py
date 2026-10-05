from pathlib import Path
import json,hashlib,subprocess,re,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-delayed-expiry-model-20261005');out=repo/'docs/scale/delayed-expiry-worker-kills-2026-10-05';out.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for name,digest in before['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',before['revision']+':'+name],cwd=repo)).hexdigest()==digest
rows=json.loads((root/'execution.json').read_text());assert len(rows)==2
for row in rows:
 assert row['source']==before['revision'] and row['exit_code']==0
 binary=root/('sim-race.test' if row['race'] else 'sim.test');assert sha(binary)==row['sha256']==row['actual_executable_sha256']
 assert 'vcs.modified=false' in row['actual_build_info'] and 'vcs.revision='+before['revision'] in row['actual_build_info']
 log=(root/(row['label']+'.log')).read_text();assert f'completed={row["seeds"]} requested={row["seeds"]}' in log
 if row['race']:assert log.count('--- PASS: TestPinnedRegressionCorpus/')==392
trace=json.loads((root/'source/sim/testdata/regressions/worker-delayed-expiry-successive-kills.json').read_text());events=trace['transport'];assert trace['workload']=='worker_delayed_expiry_successive_kills' and trace['seed']==42
checks=[x for x in events if x['operation']=='diagnostic_latency_gate'];assert [x['outcome'] for x in checks]==['within_30s_16000ms','misses_30s_34500ms']
assert len([x for x in events if x['operation']=='worker_sigkill'])==3
assert len([x for x in events if x['operation']=='kv_expiry_visibility_delay' and x['outcome']=='cleared_after_expiry'])==1
summary={'source':before['revision'],'selected_git_source_and_pins_verified':len(before['files']),'normal_new_workload_completed_seeds':100000,'race_new_workload_completed_seeds':1000,'race_exact_pinned_regressions':392,'timing_combinations_checked_by_named_test':9,'independently_checked_pin_gate_events':checks,'actual_model_binaries_and_builds_verified':True,'existing_workloads_default_expiry_delay_unchanged':True,'production_configuration_changed':False,'models_full_worker_goroutines':False,'models_nats_expiry_or_raft_internals':False,'historic_cause_confirmed':False,'failed_native_latency_parent_still_unqualified':True,'scope':'Added workload and zero-delay compatibility pins only; no new full current-source Tier1/matrix/soak release claim.'}
(root/'independent-review.json').write_text(json.dumps(summary,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py');shutil.copy2(root/'independent-review.json',out/'independent-review.json');shutil.copy2(__file__,out/'executed-review.py');shutil.copy2('/tmp/js-wf-run-delayed-expiry-model-20261005.py',out/'executed-producer.py');print(json.dumps(summary))
