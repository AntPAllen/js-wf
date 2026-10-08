import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-repair-2026-10-08/qualification'
source='f5fa502daeb108951587bb1f6ec5cfe63963faef'
expected={
 'components':{'js-wf/journal':11,'js-wf/reconcile':5,'js-wf/worker':2},
 'client':{'js-wf/client':12},
 'pins':{'js-wf/sim':1},
}
pins={p.name for p in (root/'sim/testdata/regressions').glob('*.json')}
assert len(pins)==710
summary=[]
for mode in ('normal','race'):
 before=json.loads((base/(mode+'-source-before.json')).read_text())
 after=json.loads((base/(mode+'-source-after.json')).read_text())
 assert before==after and before['source']==source
 for path,digest in before['inputs'].items():
  assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,path
  assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+path])).hexdigest()==digest,path
 commands=json.loads((base/(mode+'-commands.json')).read_text())
 assert commands['source']==source and [c['name'] for c in commands['commands']]==list(expected)
 for command in commands['commands']:
  assert command['exit_code']==0,command
  rows=[json.loads(l) for l in (base/(mode+'-'+command['name']+'.jsonl')).read_text().splitlines()]
  assert not any(r.get('Action') in ('fail','skip','build-fail') for r in rows)
  assert not any('WARNING: DATA RACE' in r.get('Output','') for r in rows)
  ends={r['Package'] for r in rows if r.get('Action')=='pass' and 'Test' not in r}
  assert ends==set(expected[command['name']]),ends
  for package,count in expected[command['name']].items():
   groups={r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Package')==package and 'Test' in r and '/' not in r['Test']}
   assert len(groups)==count,(package,groups)
  passed={r['Test'] for r in rows if r.get('Action')=='pass' and 'Test' in r}
  if command['name']=='pins':
   assert {n.split('/',1)[1] for n in passed if n.startswith('TestPinnedRegressionCorpus/')}==pins
  if command['name']=='client':
   cuts={n for n in passed if n.startswith('TestGraphCanonicalSignalPublicationRecoveryCuts/')}
   assert len({n for n in cuts if n.count('/')==2})==112
  if command['name']=='components':
   for replica in (1,3):
    assert 'TestNativeGraphReconcileCanonicalSignals/R'+str(replica) in passed
    assert 'TestNativeCanonicalSignalQueueWorkerReplay/R'+str(replica) in passed
    for asynchronous in ('true','false'):
     for external in ('true','false'):
      assert f'TestNativeCanonicalSignalQueueChildTransferAndReplay/R{replica}/async={asynchronous}/external-result={external}' in passed
   for seed in range(1,17):
    assert 'TestGraphCanonicalSignalScanBoundedRestart/'+str(seed) in passed
    assert 'TestGraphCanonicalSignalBoundRepairDuringPreparedAppend/'+str(seed) in passed
   for cut in ('index','token','name','hash','reference','missing_body','unknown_metadata','healthy'):
    assert 'TestGraphCanonicalSignalConsumptionProgressRequiresOwnedInput/'+cut in passed
  summary.append(dict(mode=mode,**command))
result={'source':source,'selected_inputs':len(before['inputs']),'pins_per_mode':len(pins),'commands':summary,'verdict':'PASS','scope':'Bounded automatic canonical Signal repair, owned consumption progress, selected Signal components, full client package, canonical worker replay/child transfer and existing pinned corpus. Full original implementation and acceptance gates remain open.'}
(base/'review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
