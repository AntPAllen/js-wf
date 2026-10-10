"""Verify directed development evidence against fixed relevant source inputs."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
inputs = json.loads((base / 'source-inputs.json').read_text())
assert {p: hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in inputs} == inputs

def read(name):
    path = base/name
    return gzip.decompress(path.read_bytes()).decode() if name.endswith('.gz') else path.read_text()

def terminals(raw, action, prefix):
    return {name for name in re.findall(r'--- '+action+r': (\S+)',raw) if name.startswith(prefix)}

prefix = 'TestGraphCheckpointPublicationBatchesAndFinalization/'
modes = ('normal','paused','renewing','tail-change','tail-race','wrong-runtime','wrong-tail','closed','expired','release-before','release-confirmed','release-unconfirmed','pointer-before','pointer-confirmed','pointer-unconfirmed')
cases = {prefix+encoding+'/'+mode for encoding in ('json','protobuf-v1') for mode in modes}
positive = read('checkpoint-frame-index-archive-race.log.gz')
assert terminals(positive,'PASS',prefix) == cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/journal\t136.092s' in positive
assert positive.count('mode=paused next=36 entry_read_attempts=38 renewals=0') == 2
assert positive.count('mode=renewing next=36 entry_read_attempts=37 renewals=18') == 2
assert positive.count('mode=closed next=3 entry_read_attempts=3') == 2
assert positive.count('mode=expired next=3 entry_read_attempts=3') == 2
budget = read('batch-budget-bypass.log')
assert terminals(budget,'FAIL',prefix) == cases
release = read('release-confirmation-bypass.log')
release_failures = {prefix+encoding+'/'+mode for encoding in ('json','protobuf-v1') for mode in ('release-before','release-unconfirmed')}
assert terminals(release,'FAIL',prefix) == release_failures
assert terminals(release,'PASS',prefix) == cases-release_failures
mutants = json.loads((base/'mutant-commands.json').read_text())
assert set(mutants) == {'batch-budget-bypass','release-confirmation-bypass'}
assert all(item['exit_code'] == 1 and item['race'] is False for item in mutants.values())
pins = read('all-853-pins.log.gz')
assert len(terminals(pins,'PASS','TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t25.088s' in pins
native = read('native20-recovery-race.log.gz')
assert 'ok  \tjs-wf/worker\t33.309s' in native and '--- FAIL:' not in native
assert '--- PASS: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true' in native
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
assert len(terminals(native,'PASS','TestGraphContinuationRepairsArchiveBeforeStage/')) == 7
assert 'original_receipts_physically_reclaimed=19 before_stage=true' in native
assert '[no tests to run]' in read('initial-native-empty-selection.log')
commands = json.loads((base/'commands.json').read_text())
assert len(commands) == 3 and all(item['exit_code'] == 0 for item in commands)
result = {'scope':'Directed development checks only; no frozen whole-plan or actual100000 acceptance.',
          'source_sha256':inputs,'publication_controls':30,'batch_limit_bypass_failures':30,
          'release_guard_bypass_failures':4,'unchanged_saved_pins':853,
          'native_budget':20,'native_checkpoints':2,'native_terminal_slot':19,'forbidden_effects':0,
          'durable_progress':False,'worker_context_seconds':15,'actual100000_accepted':False}
(base/'development-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
