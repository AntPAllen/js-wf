import pathlib,json,hashlib,subprocess,tarfile,tempfile
repo=pathlib.Path('/home/exedev/js-wf'); base=repo/'docs/scale/current-tier2-matrix-2026-10-04'; out=pathlib.Path(__file__).parent
revision='c4fed061bc614488d4f89b53b216b756490f7da0'
rows=[]; coverage=[]; common=None
for d in sorted(base.glob('consumer-*'),key=lambda p:int(p.name.split('-')[1])):
 if not (d/'manifest.json').exists():continue
 manifest=json.loads((d/'manifest.json').read_text()); summary=json.loads((d/'summary.json').read_text())
 with tempfile.TemporaryFile(dir=out) as archive:
  h=hashlib.sha256(); size=0
  for part in manifest['parts']:
   path=d/part['path']; data=path.read_bytes() if path.exists() else subprocess.check_output(['git','show','HEAD:'+str(path.relative_to(repo))],cwd=repo)
   assert len(data)==part['bytes'] and hashlib.sha256(data).hexdigest()==part['sha256'],path
   archive.write(data);h.update(data);size+=len(data)
  assert size==manifest['archive_bytes'] and h.hexdigest()==manifest['archive_sha256']
  archive.seek(0);seen=set(); report=None; binary_hash=None; dep={}
  with tarfile.open(fileobj=archive,mode='r|gz') as tar:
   for member in tar:
    assert member.isfile() and member.name not in seen;seen.add(member.name)
    ledger=manifest['files'][member.name]; dig=hashlib.sha256(); retained=bytearray(); capture=member.name=='independent-review.json' or member.name.startswith('model-source/')
    f=tar.extractfile(member)
    for b in iter(lambda:f.read(1024*1024),b''):
     dig.update(b)
     if capture:retained.extend(b)
    assert member.size==ledger['bytes'] and dig.hexdigest()==ledger['sha256'],member.name
    if member.name=='independent-review.json':report=json.loads(retained)
    if member.name=='history-review':binary_hash=dig.hexdigest()
    if member.name.startswith('model-source/'):
     name=member.name.removeprefix('model-source/');expected=subprocess.check_output(['git','show',revision+':'+name],cwd=repo)
     assert bytes(retained)==expected,name
     dep[name]=dig.hexdigest()
  assert seen==set(manifest['files']) and report
  assert report['revision']==revision and report['run']==37149506857 and report['row']=='consumer'
  assert report['shard_qualified'] and report['all_three_independent_history_models_pass']
  assert binary_hash==report['model_binary_sha256'] and report['model_binary_retained']
  assert dep==report['model_dependency_sha256'] and len(dep)==45
  if common is None:common=dep
  assert common==dep
  seeds=report['seeds'];assert [s['seed'] for s in seeds]==list(range(report['first'],report['last']+1))
  for s in seeds:
   assert s['independent_history_output']==''.join(f"whole {name} operations={s['raw_history_operations']} verdict=Ok error=<nil>\n" for name in ['starts','signals','results'])
   assert s['raw_faults_verified']==19
   assert all(c['terminal_p99_seconds']<30 for c in s['report']['cells'].values())
   assert all(c['p99_seconds']<10 for c in s['report']['progress'].values())
  assert sum(s['report']['invocations'] for s in seeds)==summary['invocations']==report['invocations']
  assert sum(s['report']['journal_entries'] for s in seeds)==summary['entries']==report['journal_entries']
  assert sum(s['raw_faults_verified'] for s in seeds)==summary['faults']==report['faults']
  coverage.extend(s['seed'] for s in seeds)
  row=dict(path=str(d.relative_to(repo)),first=report['first'],last=report['last'],job=report['job'],artifact=report['artifact'],archive_sha256=manifest['archive_sha256'],archive_bytes=size,members=len(seen),invocations=report['invocations'],entries=report['journal_entries'],faults=report['faults'],history_operations=sum(s['raw_history_operations'] for s in seeds),terminal_p99=max(c['terminal_p99_seconds'] for s in seeds for c in s['report']['cells'].values()),progress_p99=max(c['p99_seconds'] for s in seeds for c in s['report']['progress'].values()))
  rows.append(row);print(json.dumps(row),flush=True)
assert coverage==list(range(1,201)) and len(rows)==17
result=dict(revision=revision,row='consumer',first=1,last=200,shards=rows,unique_complete_coverage=True,all_archive_members_verified=True,all_actual_models_retained_and_verified=True,model_dependency_sha256=common,invocations=sum(r['invocations'] for r in rows),entries=sum(r['entries'] for r in rows),faults=sum(r['faults'] for r in rows),history_operations=sum(r['history_operations'] for r in rows),terminal_p99=max(r['terminal_p99'] for r in rows),progress_p99=max(r['progress_p99'] for r in rows),qualifies_executed_source_row=True,qualifies_current_main_full_matrix=False,qualifies_24h=False,physical_stores_reopened=False)
(out/'row-qualification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['shards','model_dependency_sha256']}),flush=True)
