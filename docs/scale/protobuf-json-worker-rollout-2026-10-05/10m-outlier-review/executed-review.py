from pathlib import Path
import json,hashlib,shutil,importlib.util,base64,datetime,math
repo=Path('/home/exedev/js-wf');original=Path('/tmp/js-wf-protobuf-json-worker-10m-20261005');fixture=original/'fixture';root=Path('/tmp/js-wf-rollout-outlier-review-20261005');root.mkdir();copy=root/'copied-wire';copy.mkdir();manifest=json.loads((original/'archive-manifest.json').read_text());observed={}
for p in fixture.iterdir():
 if p.is_file() and (p.name.startswith('rollout-') or p.name.endswith('-encoding.json') or p.name in ['process-evidence.json','journal-rollout.json','latencies.json','faults.json'] or p.name.endswith('-dispatch.jsonl')):
  digest=hashlib.sha256(p.read_bytes()).hexdigest();key=str(p.relative_to(original));assert manifest['files'][key]==digest,key;observed[key]=digest;shutil.copy2(p,copy/p.name)
(root/'original-binding.json').write_text(json.dumps({'original_archive_sha256':manifest['archive_sha256'],'files':observed},indent=2)+'\n')
spec=importlib.util.spec_from_file_location('wirechecker',repo/'scripts/check-journal-rollout.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
wire=module.check(copy,{'invocations':392})
lat=json.loads((copy/'latencies.json').read_text());short=sorted((x for x in lat if x['type']=='matrixshort' and x['event']=='terminal'),key=lambda x:x['delay_ns']);assert len(short)==56
outlier=short[math.ceil(.99*len(short))-1];assert outlier['delay_ns']==33910941280
proof=json.loads((copy/f"rollout-matrixshort-{outlier['id']}.json").read_text());assert proof['protobuf_worker_entries']==0 and proof['json_worker_entries']==4 and not proof['mixed_worker_entries']
decoded=[json.loads(base64.b64decode(x['wire_base64'],validate=True)) for x in proof['records']];assert [(x['index'],x['kind']) for x in decoded]==[(0,'Started'),(1,'StepRequested'),(2,'StepCompleted'),(3,'Completed')]
dispatch=[]
for p in copy.glob('*-dispatch.jsonl'):
 for line in p.read_text().splitlines():
  record=json.loads(line)
  if record['ID']==outlier['id'] and record['Type']=='matrixshort':dispatch.append(record)
dispatch.sort(key=lambda x:x['At']);owners={x['Worker'] for x in dispatch if x['Stage']=='lease_acquired'};kills=[f for f in json.loads((copy/'faults.json').read_text()) if f['worker'] in owners];assert len(kills)==3 # successor killed later too; retain all observed cuts
sessions=json.loads((copy/'process-evidence.json').read_text());watch=[json.loads(x) for x in Path('/tmp/js-wf-protobuf-json-worker-10m-live-20261005/sdks.jsonl').read_text().splitlines()];watchpids={x['pid'] for x in watch};sessionpids={x['pid'] for x in sessions}
review={'wire_check':wire,'short_terminal_p99':outlier,'short_terminal_sample_count':len(short),'short_terminal_above_30s':sum(x['delay_ns']>=30_000_000_000 for x in short),'outlier_raw_decoded_records':decoded,'outlier_dispatch_timeline':dispatch,'outlier_owner_faults':kills,'worker_session_count':len(sessionpids),'actual_observer_matched_worker_pids':len(watchpids & sessionpids),'unobserved_worker_pids':sorted(sessionpids-watchpids),'inference':'Outlier uses only JSON; two owners were killed before completion, with delivery 2 lease-held then delivery 3 acquired before second kill. This evidence does not prove complete latency causality or qualify the failed parent.','native_rerun':False,'failed_parent_remains_unqualified':True}
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py');shutil.copy2(repo/'scripts/check-journal-rollout.py',root/'executed-wire-checker.py');print(json.dumps(review['wire_check']));print('worker_observer',review['actual_observer_matched_worker_pids'],'/',review['worker_session_count'])
