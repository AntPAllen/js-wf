import gzip,hashlib,io,json,subprocess
from pathlib import Path
out=Path(__file__).resolve().parent
old=Path('docs/scale/graph-classified-entry-race-failure-2026-10-11')
proof=json.loads((out/'copy-verified.json').read_text())
assert proof['verified'] and proof['source_files']==866 and proof['source_bytes']==685671380
assert proof['source_census_sha256']==hashlib.sha256((old/'native-files.json.gz').read_bytes()).hexdigest()
files=json.loads(gzip.decompress((old/'native-files.json.gz').read_bytes()));source=Path(proof['source']).parent
for name,item in files.items():
 p=source/name;assert not p.is_symlink() and p.stat().st_size==item['bytes']
 assert hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'],name
for name in ('build','inspect','challenge-build','challenge','cold-after-challenge'):
 receipt=json.loads((out/(name+'-command.json')).read_text());assert receipt['actual_exit']==0 and receipt['end_ns']>receipt['start_ns']
 if name in ('build','challenge-build'):
  assert receipt['source_sha256']==hashlib.sha256((out/('challenge.go.txt' if name=='challenge-build' else 'inspect.go.txt')).read_bytes()).hexdigest()
 else:assert hashlib.sha256(Path(receipt['command'][0]).read_bytes()).hexdigest()==receipt['binary_sha256']
assert '-race=true' in json.loads((out/'challenge-command.json').read_text())['build']
inspection=json.loads((out/'inspection.json').read_text());challenge=json.loads((out/'challenge.json').read_text());cold=json.loads((out/'cold-after-challenge.json').read_text())
assert inspection['Before']==inspection['After']==challenge['Before']==challenge['After']
assert inspection['Before']['JournalCount']==inspection['Before']['JournalTail']==40959
assert inspection['Before']['Kind']=='StepCompleted' and inspection['Before']['Checkpoint'] is None
assert inspection['InvocationSequence']==1 and inspection['Input'] is None
assert inspection['FailedIndexAbsent'] and inspection['FailedIndexRecord'] is None
assert challenge['Records']==inspection['Records'] and len(inspection['Records'])==8
for i,record in enumerate(inspection['Records']):
 assert record['index']==40951+i and record['sequence']==40952+i and record['epoch']==1
 assert record['kind']==('StepRequested' if i%2==0 else 'StepCompleted')
 if i%2==0:assert record['payload']['kind']=='state_set' and record['payload']['name']=='padding'
 else:assert record['payload']['result']=={'found':True,'value':20474+i//2}
assert challenge['ChallengeStatus']==cold['Before']==cold['After']
assert cold['Before']['JournalCount']==cold['Before']['JournalTail']==40961
assert cold['Before']['Kind']=='StepCompleted' and cold['Before']['Checkpoint'] is None
assert not cold['FailedIndexAbsent'] and cold['FailedIndexRecord']==challenge['ChallengeRecords'][0]
assert len(challenge['Appends'])==len(challenge['ChallengeRecords'])==2
for i,(event,record) in enumerate(zip(challenge['Appends'],challenge['ChallengeRecords'])):
 assert event['Index']==record['index']==40959+i and event['Sequence']==record['sequence']==40960+i
 assert event['Epoch']==record['epoch'] and event['Epoch']>1
 assert 0<event['DurationNS']<15_000_000_000 and event['Requests']>0
 assert event['Kind']==record['kind']==('StepRequested' if i==0 else 'StepCompleted')
 assert record in cold['Records']
request=challenge['ChallengeRecords'][0]['payload']
assert request['kind']=='state_set' and request['name']=='padding' and request['input_hash']==hashlib.sha256(b'20478').hexdigest()
assert challenge['ChallengeRecords'][1]['payload']['result']=={'found':True,'value':20478}
assert challenge['ChallengeRecords'][0]['epoch']==challenge['ChallengeRecords'][1]['epoch']
copy=Path(proof['copy']);after=json.loads((out/'copy-after-files.json').read_text())
assert set(after)=={str(p.relative_to(copy)) for p in copy.rglob('*') if p.is_file()}
for name,item in after.items():
 p=copy/name;assert not p.is_symlink() and p.stat().st_size==item['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'],name
snapshot=json.loads((out/'reader-source-after.json').read_text());assert snapshot['source']==subprocess.check_output(['git','rev-parse','aedc09c'],text=True).strip()
names=subprocess.check_output(['git','ls-tree','-r','--name-only',snapshot['source']],text=True).splitlines()
assert set(snapshot['files'])=={name for name in names if name.endswith('.go') or name in ('go.mod','go.sum')}
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(snapshot['source']+':'+n+'\n' for n in snapshot['files']).encode()))
for name,sha in snapshot['files'].items():
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==sha==hashlib.sha256(Path(name).read_bytes()).hexdigest(),name
result=dict(verified=True,original_store_files_unchanged=866,original_store_bytes_unchanged=685671380,original_failed_index_absent=True,closed_original_count=40959,original_suffix_records=8,copy_challenge_count=40961,new_sdk_records=2,challenge_race_instrumented=True,new_lease_epoch=challenge['Appends'][0]['Epoch'],append_seconds=[x['DurationNS']/1e9 for x in challenge['Appends']],append_requests=[x['Requests'] for x in challenge['Appends']],cold_reopen_preserves_new_records=True,post_execution_reader_git_inputs=len(snapshot['files']),copy_files=len(after),copy_bytes=sum(x['bytes'] for x in after.values()),full_history_audited=False,full_workflow_recovered=False,original_100000_entry_race_accepted=False,deadline_cause_confirmed=False)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
