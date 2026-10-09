import hashlib, json, pathlib
base = pathlib.Path(__file__).resolve().parent
model={'TestGraphReplaySnapshotOwnsInputsAndRejectedSignals','TestGraphCanonicalSignalReplayReadsJournalOwnedBodyOnce'}
native={'TestGraphOperatorRejectsPartialOrLegacyOnlySelection','TestNativeCanonicalGraphOperatorCommands'}
child={'TestNativeCanonicalSignalQueueChildTransferAndReplay'}
expected={'final-normal.jsonl':native|model|child|{'TestOperatorCommands','TestOperatorCommandsInJetStreamDomain'},'final-race.jsonl':native|model|child|{'TestOperatorCommands','TestOperatorCommandsInJetStreamDomain'}}
review={}
for name,groups in expected.items():
 data=(base/name).read_bytes(); rows=[json.loads(l) for l in data.splitlines()]
 assert not any(r['Action']=='fail' or 'WARNING: DATA RACE' in r.get('Output','') for r in rows),name
 passed={r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}
 assert {t for t in passed if '/' not in t}==groups,(name,passed)
 terminal=[r for r in rows if 'Test' not in r and r['Action'] in ('pass','fail')]
 assert len(terminal)==2 and all(r['Action']=='pass' for r in terminal) and {r['Package'] for r in terminal}=={'js-wf/cmd/wf','js-wf/worker'},name
 if True:
  assert all('TestNativeCanonicalGraphOperatorCommands/'+case in passed for case in ('R1','R3Domain'))
  diagnostics=[r.get('Output','') for r in rows if 'canonical online/offline replay after source/blob deletion' in r.get('Output','')]
  assert len(diagnostics)==2 and all('wrong=0' in s for s in diagnostics),name
 if True:
  assert all('TestGraphReplaySnapshotOwnsInputsAndRejectedSignals/'+case in passed for case in ('consumed','rejected','unreadable','forged-source','release-unknown','forged-terminal')),name
 if True:
  assert sum('canonical snapshot and offline parent replay survived child/source collection' in r.get('Output','') for r in rows)==8,name
  for replica in (1,3):
   for async_ in ('true','false'):
    for external in ('true','false'):
     assert f'TestNativeCanonicalSignalQueueChildTransferAndReplay/R{replica}/async={async_}/external-result={external}' in passed,name
 review[name]={'sha256':hashlib.sha256(data).hexdigest(),'elapsed_by_package':{r['Package']:r['Elapsed'] for r in terminal},'groups':sorted(groups)}
inputs=json.loads((base/'inputs.json').read_text())
root=base.parents[2]
for path,digest in inputs['files'].items():
 assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,path
review['source_observation']='Selected component inputs observed after compilation, checked unchanged at review; no frozen-source claim.'
review['scope']='Development component evidence only; frozen complete and extended qualification remain separate.'
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
