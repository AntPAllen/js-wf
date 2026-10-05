from pathlib import Path
import json,hashlib,subprocess,tarfile,tempfile,re
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/current-tier3-block-delay-2026-10-04'
out=base/'coverage-1-156-and-196-200';out.mkdir(exist_ok=True)
revision='79915ca41a5c5a23b9997eea5f3f66d82530ee30'
expected=[(f,f+12) for f in range(1,157,13)]+[(196,200)]
rows=[];coverage=[];common=None
for first,last in expected:
 d=base/f'seeds-{first}-{last}';manifest=json.loads((d/'manifest.json').read_text());summary=json.loads((d/'summary.json').read_text())
 assert (summary['first'],summary['last'])==(first,last) and summary['source']==revision
 with tempfile.TemporaryFile(dir=out) as archive:
  h=hashlib.sha256();size=0
  for part in manifest['parts']:
   data=(d/part['path']).read_bytes();assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256']
   archive.write(data);h.update(data);size+=len(data)
  assert size==manifest['archive_bytes'] and h.hexdigest()==manifest['archive_sha256']
  archive.seek(0);seen=set();retained={};deps={};history_ops={};model_stdout={};binary_hash=None
  with tarfile.open(fileobj=archive,mode='r|gz') as tar:
   for member in tar:
    assert member.isfile() and member.name not in seen;seen.add(member.name)
    ledger=manifest['files'][member.name];dig=hashlib.sha256();buf=bytearray()
    capture=member.name in ('row-review.json','model-review.json','model-binary.json','model-dependencies.json') or member.name.startswith(('model-source/','model-seed-'))
    f=tar.extractfile(member)
    if re.fullmatch(r'raw/tier3-matrix-range/seed-\d+/tier3-mixed-journal/history.jsonl',member.name):
     seed=int(member.name.split('/')[2].split('-')[1]);operations=0
     for line in f:dig.update(line);operations+=bool(line.strip())
     history_ops[seed]=operations
    else:
     for b in iter(lambda:f.read(1024*1024),b''):
      dig.update(b)
      if capture:buf.extend(b)
    assert member.size==ledger['bytes'] and dig.hexdigest()==ledger['sha256'],member.name
    if member.name in ('row-review.json','model-review.json','model-binary.json','model-dependencies.json'):retained[member.name]=json.loads(buf)
    if member.name=='history-review':binary_hash=dig.hexdigest()
    if member.name.startswith('model-source/'):
     name=member.name.removeprefix('model-source/');assert bytes(buf)==subprocess.check_output(['git','show',revision+':'+name],cwd=repo);deps[name]=dig.hexdigest()
    if member.name.startswith('model-seed-'):model_stdout[member.name]=bytes(buf).decode()
  assert seen==set(manifest['files'])
  report=retained['row-review.json'];models=retained['model-review.json'];binary=retained['model-binary.json'];dependency=retained['model-dependencies.json']
  assert report['source']==models['source']==binary['source']==dependency['source']==revision
  assert report['run_id']==37164231641 and report['job_id']==summary['job'] and report['artifact_id']==summary['artifact']
  assert report['row']=='block_delay' and report['shard_qualified'] and report['duration_seconds']==600
  assert (report['first_seed'],report['last_seed'],report['seeds'])==(first,last,last-first+1)
  assert binary_hash==binary['sha256'] and len(deps)==45 and dependency['files']==deps
  if common is None:common=deps
  assert common==deps and models['all_three_models_exact_ok'] and models['actual_dependencies']==45
  for name,digest in report['artifact_sha256'].items():assert manifest['files']['raw/'+name]['sha256']==digest
  for file,key in [('run.json','run'),('job.json','job'),('artifact.json','artifact_metadata'),('job.log','job_log')]:assert manifest['files'][file]['sha256']==report['input_sha256'][key]
  assert [s['seed'] for s in report['reports']]==[s['seed'] for s in models['reports']]==list(range(first,last+1))
  for case,model in zip(report['reports'],models['reports']):
   seed=case['seed'];ops=history_ops[seed]
   assert case['duration_seconds']==600 and not case['shortened_smoke'] and case['confirmed_faults']==19
   assert case['block_disk_artifact_checks']['confirms_dm_delay'] and case['block_disk_artifact_checks']['confirmed_five_second_delay_intervals']==19
   assert case['checkpoint_audit_checks']['all_expected_checkpoints_present']
   assert all(c['terminal_p99_seconds']<30 and c['progress_p99_seconds']<30 for c in case['cells'].values())
   assert model['operations']==ops and model['all_three_exact_ok']
   assert model_stdout[f'model-seed-{seed}.stdout']==''.join(f'whole {name} operations={ops} verdict=Ok error=<nil>\n' for name in ('starts','signals','results'))
   assert not model_stdout[f'model-seed-{seed}.stderr']
  for key in ('invocations','journal_entries'):assert summary[key]==report[key]==sum(s[key] for s in report['reports'])
  assert summary['faults']==report['confirmed_faults']==sum(s['confirmed_faults'] for s in report['reports'])
  assert summary['model_operations']==models['operations']==sum(history_ops.values())
  audits=sum(s['checkpoint_audit_checks']['completed_cohort_audits'] for s in report['reports'])
  coverage.extend(s['seed'] for s in report['reports'])
  row={**summary,'archive_sha256':manifest['archive_sha256'],'archive_bytes':size,'members':len(seen),'completed_cohort_audits':audits,'path':str(d.relative_to(repo))}
  rows.append(row);print(json.dumps(row),flush=True)
assert coverage==list(range(1,157))+list(range(196,201)) and len(set(coverage))==161
result=dict(source=revision,row='block_delay',accepted_seeds=len(coverage),continuous_first=1,continuous_last=156,additional_seeds=list(range(196,201)),missing_seeds=list(range(157,196)),all_archive_members_and_parts_verified=True,all_actual_models_and_dependencies_verified=True,model_dependency_sha256=common,shards=rows,**{k:sum(r[k] for r in rows) for k in ('invocations','journal_entries','faults','model_operations','completed_cohort_audits')},worst_terminal_p99_seconds=max(r['worst_terminal_p99_seconds'] for r in rows),worst_progress_p99_seconds=max(r['worst_progress_p99_seconds'] for r in rows),qualifies_recorded_source_seeds=True,qualifies_full_row=False,qualifies_final_source=False,qualifies_full_matrix=False,qualifies_24h=False,workload_sdk_retained=False,physical_stores_retained=False,final_integrity_and_drain_scope='named-test assertions')
(out/'coverage-qualification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ('shards','model_dependency_sha256')}),flush=True)
