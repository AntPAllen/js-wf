"""Verify bounded staging development controls, without whole-plan acceptance."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
inputs = json.loads((base/'source-inputs.json').read_text())
assert {p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in inputs} == inputs

def read(name):
    data = (base/name).read_bytes()
    return (gzip.decompress(data) if name.endswith('.gz') else data).decode()

def terminals(raw, action, prefix):
    return {name for name in re.findall(r'--- '+action+r': (\S+)',raw) if name.startswith(prefix)}

batches = 'TestGraphPrefixCompactionStageBatchesAndResumption/'
modes = ('normal','resume-every-record','inherited-archive','cancel','put-drop','put-lost','ready-lost','append','retire','reader','collector','stale-target')
batch_cases = {batches+mode for mode in modes}
headers = 'TestGraphPrefixCompactionStageRejectsCheckpointMutations/'
header_cases = {headers+mode for mode in ('duplicate','alias','unknown','trailing','oversize','schema','head','source','progress','cut','readers','streams')}
forged = 'TestGraphPrefixCompactionStageDoesNotCertifyForgedPrefix/'
forged_cases = {forged+mode for mode in ('data','payload')}
positive = read('compaction-stage-all-race.log.gz')
assert terminals(positive,'PASS',batches) == batch_cases
assert terminals(positive,'PASS',headers) == header_cases
assert terminals(positive,'PASS',forged) == forged_cases
assert '--- PASS: TestGraphPrefixCompactionStageReturnedPlanOwnership' in positive
assert '--- PASS: TestGraphPrefixCompactionStageSeededBudgets' in positive
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/internal/graphpublication\t19.997s' in positive
assert 'mode=resume-every-record source_records=12 batches=12' in positive
budget = read('batch-budget-bypass.log')
assert terminals(budget,'FAIL',batches) == batch_cases
binding = read('source-binding-bypass.log')
assert terminals(binding,'FAIL',batches) == batch_cases-{batches+'stale-target'}
assert terminals(binding,'PASS',batches) == {batches+'stale-target'}
assert terminals(binding,'FAIL',headers) == {headers+'source'}
assert terminals(binding,'PASS',headers) == header_cases-{headers+'source'}
assert terminals(read('content-comparison-bypass.log'),'FAIL',forged) == forged_cases
pins = read('all-853-pins.log.gz')
assert len(terminals(pins,'PASS','TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t7.004s' in pins
native = read('native20-recovery-race.log.gz')
assert 'ok  \tjs-wf/worker\t20.098s' in native and '--- FAIL:' not in native
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
assert len(terminals(native,'PASS','TestGraphContinuationRepairsArchiveBeforeStage/')) == 7
restart = read('native-stage-restart-race.log')
assert '--- FAIL:' not in restart and 'ok  \tjs-wf/internal/graphpublication\t11.977s' in restart
assert terminals(restart,'PASS','TestNativeGraphCompactionStageStoreRestart/') == {'TestNativeGraphCompactionStageStoreRestart/R1','TestNativeGraphCompactionStageStoreRestart/R3'}
for replicas in (1,3):
    assert f'replicas={replicas} checkpoint_next=1 records=4 archive=2 live=2 original_objects_deleted=11' in restart
assert '--- FAIL: TestNativeGraphCompactionStageStoreRestart/R3' in read('initial-native-restart-failure.log')
commands = json.loads((base/'commands.json').read_text())
assert len(commands) == 4 and all(row['exit_code'] == 0 for row in commands)
mutants = json.loads((base/'mutant-commands.json').read_text())
assert len(mutants) == 3 and all(row['exit_code'] == 1 and not row['race'] for row in mutants.values())
result = dict(scope='Directed development evidence only; no frozen whole-plan or actual100000 acceptance.',
              source_sha256=inputs, batch_controls=12, descriptor_controls=12,
              forged_prefix_controls=2, sampled_local_schedules=32,
              required_bypass_failures=dict(batch=12,source_binding=12,content=2),
              unchanged_saved_pins=853, native_runtime_budget=20,
              native_staging_restart_replicas=[1,3],original_objects_deleted_per_restart=11,
              portable_staging_input=True,verified_prefix_certificate=False,
              runtime_persistence=False,intent_renewal=False,bounded_final_commit=False,
              worker_context_seconds=15,actual100000_accepted=False)
(base/'development-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
