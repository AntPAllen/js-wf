import hashlib,json,pathlib
base=pathlib.Path(__file__).resolve().parent
root=base.parents[2]
review={}
for name in ('final-cli-normal.jsonl','final-cli-race.jsonl','project-domain-normal.jsonl','project-domain-race.jsonl','reader-recovery-controls-normal.jsonl','reader-recovery-controls-race.jsonl','namespace-controls-race.jsonl'):
 data=(base/name).read_bytes();rows=[json.loads(l) for l in data.splitlines()]
 assert not any(r['Action']=='fail' or 'WARNING: DATA RACE' in r.get('Output','') for r in rows),name
 terminal=[r for r in rows if 'Test' not in r and r['Action'] in ('pass','fail')]
 assert terminal and all(r['Action']=='pass' for r in terminal),name
 passed={r['Test'] for r in rows if r['Action']=='pass' and 'Test' in r}
 if name.startswith(('final-cli','project-domain')):
  expected={'TestNativeCanonicalGraphOperatorCommands'}
  if name.startswith('final-cli'):expected|={'TestGraphOperatorRejectsPartialOrLegacyOnlySelection','TestOperatorCommands','TestOperatorCommandsInJetStreamDomain'}
  assert {t for t in passed if '/' not in t}==expected,name
  for case in ('R1','R3Domain'):assert 'TestNativeCanonicalGraphOperatorCommands/'+case in passed,name
  assert sum('canonical CLI project/list/lag and isolated page while worker active' in r.get('Output','') for r in rows)==2,name
  if name.startswith('project-domain'):assert sum('graph project subprocess joined; legacy journal requests zero and domain API verified' in r.get('Output','') for r in rows)==2,name
 if name.startswith('reader-recovery-controls'):
  for case in ('ordinary','catalog-unknown','source-forged','lease-held','lease-unknown','retired'):
   assert 'TestGraphVisibilityCanonicalRowsAndUncertainty/'+case in passed,name
  required={'TestGraphVisibilityConfiguration','TestGraphOperatorRejectsPartialOrLegacyOnlySelection','TestVisibilityPageCursorAndBounds','TestProjectionReaderSeededRetainedHolesAndWatermark','TestProjectionReaderFailureDoesNotCertifyEmptySource','TestProjectionReaderCancellationWhileOutputBlocked','TestProjectionReaderZeroPendingWithUnobservedDelivery','TestProjectionReaderNativeConsumerDeletion','TestProjectionReaderMissingConsumerDoesNotCertifyEmptySource'}
  assert required.issubset(passed),(name,required-passed)
 if name.startswith('namespace-controls'):assert {'TestGraphVisibilityConfiguration','TestGraphOperatorRejectsPartialOrLegacyOnlySelection'}.issubset(passed),name
 review[name]={'sha256':hashlib.sha256(data).hexdigest(),'elapsed_by_package':{r['Package']:r['Elapsed'] for r in terminal},'top_level_pass':sorted(t for t in passed if '/' not in t)}
negative=[json.loads(l) for l in (base/'counterfactual.jsonl').read_text().splitlines()]
assert any(r['Action']=='fail' and r.get('Test')=='TestGraphVisibilityCanonicalRowsAndUncertainty/ordinary' for r in negative)
assert any('unchanged canonical refresh pinned or trusted query bytes' in r.get('Output','') for r in negative)
original=[json.loads(l) for l in (base/'cli-race.jsonl').read_text().splitlines()]
assert any(r['Action']=='fail' and 'Test' not in r for r in original)
assert any('signal publish outcome unknown: context deadline exceeded' in r.get('Output','') for r in original)
reader_negative=[json.loads(l) for l in (base/'reader-counterfactual.jsonl').read_text().splitlines()]
assert any(r['Action']=='fail' and r.get('Test')=='TestProjectionReaderMissingConsumerDoesNotCertifyEmptySource' for r in reader_negative)
for path,digest in json.loads((base/'inputs.json').read_text())['files'].items():assert hashlib.sha256((root/path).read_bytes()).hexdigest()==digest,path
review['scope']='Development component checks; source observed after compilation. Earlier run scopes preserved. Full/extended/native-scale/adoption/release qualification remains separate.'
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
