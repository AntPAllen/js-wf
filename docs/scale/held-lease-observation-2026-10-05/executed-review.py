from pathlib import Path
import json,hashlib,subprocess,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-held-observation-20261005');out=repo/'docs/scale/held-lease-observation-2026-10-05';out.mkdir()
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for n,d in before['files'].items():
 assert sha(root/'source'/n)==d
 assert hashlib.sha256(subprocess.check_output(['git','show',before['revision']+':'+n],cwd=repo)).hexdigest()==d
rows=json.loads((root/'execution.json').read_text());assert len(rows)==3
for row in rows:
 assert row['source']==before['revision'] and row['exit_code']==0
 package=Path(row['test'][0]).name.split('.')[0];assert sha(root/(package+'.test'))==row['sha256']==row['actual_sha256']
 assert 'vcs.revision='+before['revision'] in row['actual_build_info'] and 'vcs.modified=false' in row['actual_build_info']
log=(root/'race-model-1k-and-pins.log').read_text();assert 'completed=1000 requested=1000' in log and log.count('--- PASS: TestPinnedRegressionCorpus/')==392
native=json.loads((root/'native/held-observations.json').read_text());o=native['observations'];assert len(o)==2
assert all(x['entry_observed'] and x['value_valid'] and x['reason']=='held_entry' and x['key']=='test.held' and x['worker']=='prior' for x in o)
assert o[1]['revision']>o[0]['revision'] and o[1]['epoch']==o[0]['epoch']>0 and o[1]['created']>o[0]['created']
assert [native[k] for k in ['Creates','Gets','Updates','Deletes']]==[2,2,0,0]
assert len(set(native['server_ids']))==3;cfg=native['stream']['config'];assert cfg['num_replicas']==3 and cfg['storage']=='file' and cfg['max_age']==12000000000
review={'source':before['revision'],'selected_git_inputs_and_pins_verified':len(before['files']),'native_r3_file_ttl_12s_observations':o,'native_acquisition_only_broker_requests':{k:native[k] for k in ['Creates','Gets','Updates','Deletes']},'native_three_unique_embedded_server_ids':native['server_ids'],'all_actual_test_binary_hashes_and_build_fields_verified':True,'worker_observer_control_and_negative_lease_controls_pass':True,'held_terminal_model_completed_race_seeds':1000,'exact_race_pins':392,'production_error_identity_and_timing_preserved':True,'extra_broker_reads':False,'native_server_execution_scope':'Three real NATS servers embedded in actual captured lease.test SDK; not separate server processes','preliminary_unqualified_failure':'Initial full-runtime provision failed before observation; failed test source and report retained, executable and temporary stores were not captured. Does not qualify provisioning or establish its cause.','historical_latency_cause_confirmed':False,'clears_full_matrices_or_24h':False}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py');shutil.copy2(root/'independent-review.json',out/'independent-review.json');shutil.copy2(root/'native/held-observations.json',out/'held-observations.json');shutil.copy2(__file__,out/'executed-review.py');shutil.copy2('/tmp/js-wf-run-held-observation-20261005.py',out/'executed-producer.py');shutil.copy2('/tmp/js-wf-preserve-read-proof-20261005.py',out/'executed-preserver.py');print(json.dumps(review))
