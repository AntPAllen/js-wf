import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-runtime-tier1-2026-10-08/complete-normal1000'
source='dd98e39c29c5e8689f06fdbdcca7e944b53c0963'
before=json.loads((base/'source-before.json').read_text())
after=json.loads((base/'source-after.json').read_text())
assert before==after and before['revision']==source
for path,digest in before['files'].items():
 assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+path])).hexdigest()==digest,path
 assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,path
result=json.loads((base/'tier1-result.json').read_text())
assert result['source']==source and result['top_level_pass']==206 and result['pinned_regressions_pass']==728
assert set(result['trace_only_skips'])=={'TestMinimizeFaultTrace','TestReplayFaultTrace'}
proof=result['per_workload_seed_proof']
assert proof['workloads']==145 and proof['first']==1 and proof['last']==1000 and proof['completed_bodies']==145000
assert len(proof['tests'])==145 and 'TestSeededGraphSignalRuntimeReplay' in proof['tests']
assert hashlib.sha256((base/'tier1-events.jsonl').read_bytes()).hexdigest()==result['events_sha256']
rows=[json.loads(l) for l in (base/'tier1-events.jsonl').read_text().splitlines()]
assert not any(r.get('Action')=='fail' for r in rows)
assert not any('WARNING: DATA RACE' in r.get('Output','') for r in rows)
ends=[r for r in rows if r.get('Action')=='pass' and 'Test' not in r]
assert len(ends)==1 and ends[0]['Package']=='js-wf/sim'
provenance=json.loads((base/'binary.json').read_text())
assert provenance['race_instrumented']==False and '-race=true' not in provenance['build_info']
binary=pathlib.Path('/home/exedev/js-wf-tier1-full145-normal1000-20261008/sim.test')
assert hashlib.sha256(binary.read_bytes()).hexdigest()==provenance['binary_sha256']
review={'source':source,'selected_inputs':len(before['files']),'top_level_pass':206,'seeded_families':145,'seeds_each':1000,'completed_seed_bodies':145000,'pins':728,'source_and_binary_unchanged':True,'verdict':'PASS','scope':'Complete normal default Tier1 suite at frozen source; complete race and extended campaigns and original wider gates remain separate.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
