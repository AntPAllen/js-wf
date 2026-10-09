import hashlib,json,pathlib,re
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-checkpoint-materialization-2026-10-09'
required={'final-materialization-race.log':['TestNativeGraphCheckpointMaterializedReferences'],'initial-race.log':['TestNativeGraphCheckpointMaterializedReferences','TestNativeGraphContinuationSDKFlow','TestNativeGraphContinuationSDKPartitionFlow']}
for log,tests in required.items():
 text=(base/log).read_text()
 assert not re.search(r'^.*--- (FAIL|SKIP):',text,re.M) and 'WARNING: DATA RACE' not in text,log
 assert not re.search(r'^FAIL',text,re.M),log
 assert re.search(r'^ok\s+js-wf/worker',text,re.M),log
 for test in tests:
  assert re.search(r'^--- PASS: '+test+' ',text,re.M),(log,test)
  for mode in ('R1','R3Domain'): assert f'--- PASS: {test}/{mode} ' in text,(log,test,mode)
assert (base/'final-materialization-race.log').read_text().count('promise aliases=2 completion_payload_edges=2 materialized_index=4 blocked_prefix_reads=0')==2
assert (base/'initial-race.log').read_text().count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records=20 result=43')==4
inputs=['worker/graph_journal.go','worker/graph_checkpoint.go','worker/graph_checkpoint_test.go','worker/graph_continuation_sdk_test.go']
review=dict(verdict='PASS',source_hashes_observed_at_review={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in inputs},log_hashes={p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in required},native_modes=['R1','R3Domain'],scope='Development checkpoint promise edge transfer and existing SDK/partition controls. Bounded worker loading, physical prefix compaction/collection and full qualification remain open.')
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
