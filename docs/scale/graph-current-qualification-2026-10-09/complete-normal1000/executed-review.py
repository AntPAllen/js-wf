import hashlib,json,pathlib,subprocess
base=pathlib.Path(__file__).resolve().parent
repo=base.parents[3]
source_root=pathlib.Path('/home/exedev/js-wf-compaction-qualification')
source='fca8264d2229144e9b3e6a70df747d1307666fe6'
before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text())
assert before==after and before['revision']==source
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source_root,text=True).strip()==source
blobs={}
for row in subprocess.check_output(['git','ls-tree','-r',source],cwd=repo).splitlines():
 meta,name=row.split(b'\t',1);blobs[name.decode()]=meta.split()[2].decode()
for path,digest in before['files'].items():
 data=(source_root/path).read_bytes()
 assert hashlib.sha256(data).hexdigest()==digest,path
 assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==blobs[path],path
result=json.loads((base/'tier1-result.json').read_text())
assert result['source']==source and result['top_level_pass']==211 and result['pinned_regressions_pass']==762
assert set(result['trace_only_skips'])=={'TestMinimizeFaultTrace','TestReplayFaultTrace'}
proof=result['per_workload_seed_proof']
assert proof['workloads']==149 and proof['first']==1 and proof['last']==1000 and proof['completed_bodies']==149000
assert len(proof['tests'])==149 and 'TestSeededGraphCheckpointCompactionReplay' in proof['tests']
assert hashlib.sha256((base/'tier1-events.jsonl').read_bytes()).hexdigest()==result['events_sha256']
rows=[json.loads(line) for line in (base/'tier1-events.jsonl').read_text().splitlines()]
assert not any(row.get('Action')=='fail' or 'WARNING: DATA RACE' in row.get('Output','') for row in rows)
ends=[row for row in rows if row.get('Action')=='pass' and 'Test' not in row]
assert len(ends)==1 and ends[0]['Package']=='js-wf/sim'
provenance=json.loads((base/'binary.json').read_text())
assert provenance['race_instrumented']==False and '-race=true' not in provenance['build_info']
binary=pathlib.Path('/home/exedev/js-wf-tier1-full149-normal1000-corrected-20261009/sim.test')
assert hashlib.sha256(binary.read_bytes()).hexdigest()==provenance['binary_sha256']
commands=json.loads((base/'commands.json').read_text())
execution=next(row for row in commands if 'test2json' in row['command'])
assert execution['working_directory']==str(source_root/'sim')
assert json.loads((base/'tier1-events.jsonl.exit.json').read_text())['exit_code']==0
review={'source':source,'verdict':'PASS complete normal default simulation','selected_inputs':len(before['files']),'groups_pass':211,'seeded_families':149,'completed_seed_bodies':149000,'pins':762,'elapsed_seconds':ends[0]['Elapsed'],'source_and_binary_unchanged':True,'scope':'Includes compaction and full authority guard at frozen fca8264. Excludes later process fixtures, CLI cursor selection and Await contention retry. Current full/race/extended and every original native/scale/migration/release requirement remain separate.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review))
