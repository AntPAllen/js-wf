import sys,json,shutil,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-retained-cohort-tier1-20261008');sys.path.insert(0,str(repo/'scripts'));sys.path.insert(0,'/tmp')
import fixture_archive
from storage_review_common import closure
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=json.loads((root/'normal100k/execution.json').read_text());log=(root/'normal100k/actual.log').read_text();assert r['exit_code']==0 and r['body_complete'] and 'PASS' in log.splitlines() and log.count('--- PASS: TestPinnedRegressionCorpus/retained-cohort-')==9
assert not any(x in log for x in ('--- FAIL:','--- SKIP:','DATA RACE'))
assert shared.sha(root/'normal100k/sim.test')==json.loads((root/'normal100k/binary.json').read_text())['sha256']
after=shared.source_inventory(r['source']);assert after==json.loads((root/'source-before.json').read_text())
(root/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');(root/'closure.json').write_text(json.dumps(closure(root),indent=2)+'\n')
shutil.copy2('/tmp/cohort-tier1-producer.log',root/'producer-tail-check-failure.log');shutil.copy2(__file__,root/'executed-offline-finalization.py')
(root/'offline-review.json').write_text(json.dumps({'normal100k_sdk_terminal_pass':True,'all_9_pins_exact_replay':True,'body_complete':True,'source_unchanged':True,'producer_failure':'Offline endswith(PASS) assumption rejected valid coverage footer after PASS. Actual SDK exited 0. Reviewed exact PASS line, summary and pin records without rerunning normal profile. Race qualified separately.'},indent=2)+'\n')
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('FINALIZED_NORMAL')
