import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-replay-io-2026-10-08/development'
checks={
 'final-worker-normal.jsonl':{'TestNativeCanonicalSignalQueueWorkerReplay','TestNativeCanonicalSignalQueueChildTransferAndReplay','TestGraphCanonicalSignalReplayReadsJournalOwnedBodyOnce'},
 'final-worker-race.jsonl':{'TestNativeCanonicalSignalQueueWorkerReplay','TestNativeCanonicalSignalQueueChildTransferAndReplay','TestGraphCanonicalSignalReplayReadsJournalOwnedBodyOnce'},
 'final-journal-normal.jsonl':{'TestGraphCanonicalSignalQueueOrderAndPersistentDuplicates','TestGraphCanonicalSignalQueueNeverSkipsUnknownOrForgedSource','TestGraphCanonicalSignalQueueConcurrentBindingAndRetirement','TestGraphCanonicalSignalQueueReplacementAndFutureFence'},
 'final-journal-race.jsonl':{'TestGraphCanonicalSignalQueueOrderAndPersistentDuplicates','TestGraphCanonicalSignalQueueNeverSkipsUnknownOrForgedSource','TestGraphCanonicalSignalQueueConcurrentBindingAndRetirement','TestGraphCanonicalSignalQueueReplacementAndFutureFence'},
 'final-pins-normal.jsonl':{'TestPinnedRegressionCorpus'},
 'final-pins-race.jsonl':{'TestPinnedRegressionCorpus'},
 'capture-1000.jsonl':{'TestSeededGraphSignalRuntimeReplay'},
}
results=[]
for name,want in checks.items():
 data=(base/name).read_bytes();events=[json.loads(s) for s in data.splitlines()]
 assert events and events[-1]['Action']=='pass' and not events[-1].get('Test'),name
 assert not any(e['Action']=='fail' or 'DATA RACE' in e.get('Output','') for e in events),name
 passes={e['Test'] for e in events if e['Action']=='pass' and e.get('Test')}
 assert {s for s in passes if '/' not in s}==want,(name,passes)
 if 'TestPinnedRegressionCorpus' in want:
  pins={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
  assert {s.split('/',1)[1] for s in passes if s.startswith('TestPinnedRegressionCorpus/')}==pins
  assert len(pins)==728
 if 'TestNativeCanonicalSignalQueueChildTransferAndReplay' in want:
  children=[s for s in passes if s.startswith('TestNativeCanonicalSignalQueueChildTransferAndReplay/') and '/external-result=' in s]
  assert len(children)==8
 if 'TestSeededGraphSignalRuntimeReplay' in want:
  output=''.join(e.get('Output','') for e in events)
  assert 'completed=1000' in output
 results.append({'log':name,'sha256':hashlib.sha256(data).hexdigest(),'groups':sorted(want),'seconds':events[-1].get('Elapsed')})
lineages=json.loads((base/'pin-lineage.json').read_text());assert len(lineages)==18
migrated={v['path'] for v in lineages}
for v in lineages:
 old=subprocess.check_output(['git','show',v['prior_source']+':'+v['path']])
 new=(root/v['path']).read_bytes()
 assert hashlib.sha256(old).hexdigest()==v['prior_sha256']
 assert hashlib.sha256(new).hexdigest()==v['current_sha256']
 old,new=json.loads(old),json.loads(new)
 assert {k:v for k,v in old.items() if k!='transport'}=={k:v for k,v in new.items() if k!='transport'}
 remaining=iter(new['transport']);wanted=next(remaining,None);removed=[]
 for event in old['transport']:
  if wanted is not None and event==wanted:wanted=next(remaining,None)
  else:
   assert event['operation']=='graph_publication_get' and event['subject'].startswith(hashlib.sha256(b'true').hexdigest()+'/') and event['sequence']==4 and event['outcome']=='ok'
   removed.append(event)
 assert wanted is None and len(removed)==v['removed_owned_queue_body_gets']
unchanged=0
for p in (root/'sim/testdata/regressions').glob('*.json'):
 rel=str(p.relative_to(root))
 if rel in migrated:continue
 assert p.read_bytes()==subprocess.check_output(['git','show','04f8dbd:'+rel]),rel
 unchanged+=1
assert unchanged==710
original=[json.loads(s) for s in (base/'original-control.jsonl').read_bytes().splitlines()]
assert original[-1]['Action']=='fail'
assert any('2' in e.get('Output','') and 'body reads' in e.get('Output','') for e in original)
report={'scope':'development controls; no frozen whole-suite claim','results':results,'migrated_pins':18,'unchanged_pins':unchanged,'expected_original_control':'FAIL: duplicate body read'}
(base/'executed-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
