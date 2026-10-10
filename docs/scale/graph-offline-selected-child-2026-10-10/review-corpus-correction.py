"""Review the actual completed corpus run; correct filename namespace only."""
import datetime,hashlib,json,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-selected-child-20261010')
checkout=Path('/home/exedev/js-wf-selected-child-qualification')
original=json.loads((base/'corpus-correction.json').read_text())
(root/'corpus-correction-original.json').write_text(json.dumps(original,indent=2)+'\n')
assert len(original['commands'])==2 and all(c['exit']==0 for c in original['commands'])
expected=sorted(p.name for p in (checkout/'sim/testdata/regressions').glob('*.json'))
assert len(expected)==841
rows=[json.loads(line) for line in (root/'corpus-corrected-events.jsonl').read_text().splitlines()]
prefix='TestPinnedRegressionCorpus/'
actual=sorted(r['Test'][len(prefix):] for r in rows if r['Action']=='pass' and r.get('Test','').startswith(prefix))
assert actual==expected and not any(r['Action'] in ('fail','skip') for r in rows)
assert any(r['Action']=='pass' and r.get('Test')=='TestPinnedRegressionCorpus' for r in rows)
assert any(r['Action']=='pass' and 'Test' not in r for r in rows)
before=json.loads((root/'source-before.json').read_text())
assert all(hashlib.sha256((checkout/name).read_bytes()).hexdigest()==digest for name,digest in before['files'].items())
assert hashlib.sha256(Path(original['binary']).read_bytes()).hexdigest()==original['binary_sha256']
properties=subprocess.check_output(['systemctl','--user','show','js-wf-selected-child-corpus-correction-20261010.service','-p','MainPID','-p','ExecMainStatus','-p','ActiveState'],text=True)
assert 'MainPID=0\n' in properties and 'ExecMainStatus=1\n' in properties
(root/'corpus-correction-supervisor-exit.txt').write_text(properties)
result=dict(source=original['source'],accepted=True,actual_inventory_exit=0,actual_corpus_exit=0,actual_supervisor_exit=1,supervisor_failure_reason='Post-test checker compared Path.stem to Go filepath.Base subtest names including .json. All841 tests passed; corrected review uses filename including extension. No tests repeated.',expected_cases=expected,passed_cases=len(actual),binary_sha256=original['binary_sha256'],source_unchanged=True,package_elapsed=next(r['Elapsed'] for r in rows if r['Action']=='pass' and 'Test' not in r),reviewed=datetime.datetime.now(datetime.timezone.utc).isoformat())
(base/'corpus-correction-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(dict(passed=result['passed_cases'],actual_corpus_exit=0,actual_supervisor_exit=1,review_accepted=True)))
