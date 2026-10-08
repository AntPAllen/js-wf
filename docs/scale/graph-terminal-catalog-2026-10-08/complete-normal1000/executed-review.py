import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-terminal-catalog-2026-10-08/complete-normal1000'
source_root=pathlib.Path('/home/exedev/js-wf-terminal-catalog-qualification')
source=subprocess.check_output(['git','rev-parse','7ee9882'],text=True).strip()
before=json.loads((base/'source-before.json').read_text())
after=json.loads((base/'source-after.json').read_text())
assert before==after and before['revision']==source
for path,digest in before['files'].items():
 assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+path])).hexdigest()==digest,path
 assert hashlib.sha256((source_root/path).read_bytes()).hexdigest()==digest,path
result=json.loads((base/'tier1-result.json').read_text())
assert result['source']==source and result['top_level_pass']==209 and result['pinned_regressions_pass']==748
assert set(result['trace_only_skips'])=={'TestMinimizeFaultTrace','TestReplayFaultTrace'}
proof=result['per_workload_seed_proof']
assert proof['workloads']==148 and proof['first']==1 and proof['last']==1000 and proof['completed_bodies']==148000
assert len(proof['tests'])==148 and {'TestSeededGraphSignalRuntimeReplay','TestSeededGraphSignalRuntimeCombinedReplay'}.issubset(proof['tests'])
assert hashlib.sha256((base/'tier1-events.jsonl').read_bytes()).hexdigest()==result['events_sha256']
rows=[json.loads(l) for l in (base/'tier1-events.jsonl').read_text().splitlines()]
assert not any(r.get('Action')=='fail' for r in rows)
assert not any('WARNING: DATA RACE' in r.get('Output','') for r in rows)
ends=[r for r in rows if r.get('Action')=='pass' and 'Test' not in r]
assert len(ends)==1 and ends[0]['Package']=='js-wf/sim'
provenance=json.loads((base/'binary.json').read_text())
assert provenance['race_instrumented']==False and '-race=true' not in provenance['build_info']
binary=pathlib.Path('/home/exedev/js-wf-tier1-full148-normal1000-20261008/sim.test')
assert hashlib.sha256(binary.read_bytes()).hexdigest()==provenance['binary_sha256']
review={'source':source,'selected_inputs':len(before['files']),'top_level_pass':209,'seeded_families':148,'seeds_each':1000,'completed_seed_bodies':148000,'pins':748,'source_and_binary_unchanged':True,'verdict':'PASS','scope':'Complete normal default Tier1 suite at frozen source; complete race and extended campaigns and original wider gates remain separate.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
