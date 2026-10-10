"""Check retained private bounded-verification development evidence."""
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

prefix = 'TestGraphPrefixCompactionCommitBatchesAndFences/'
modes = ('normal','inherited','record-deadline','node-deadline','append','reader','retire','collector-records','collector-nodes','collector-at-cas','node-grant','drop','lost','unconfirmed','plan-ownership')
cases = {prefix+mode for mode in modes}
positive = read('compaction-and-native-restart-race.log.gz')
assert terminals(positive,'PASS',prefix) == cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/internal/graphpublication\t47.045s' in positive
assert 'mode=normal records=12 nodes=19' in positive
assert terminals(positive,'PASS','TestNativeGraphCompactionStageStoreRestart/') == {'TestNativeGraphCompactionStageStoreRestart/R1','TestNativeGraphCompactionStageStoreRestart/R3'}
for replicas in (1,3):
    assert f'NATIVE_COMPACTION_COMMIT replicas={replicas} compared_records=4 verified_nodes=6 record_budget=1 node_budget=1' in positive
    assert f'replicas={replicas} checkpoint_next=1 records=4 archive=2 live=2 original_objects_deleted=11' in positive
assert terminals(read('record-budget-bypass.log'),'FAIL',prefix) == cases
early = {prefix+mode for mode in ('append','reader','retire','collector-records')}
nodes = read('node-budget-bypass.log')
assert terminals(nodes,'FAIL',prefix) == cases-early
assert terminals(nodes,'PASS',prefix) == early
grants = read('node-grant-bypass.log')
grant_failures = {prefix+mode for mode in ('node-grant','node-deadline')}
assert terminals(grants,'FAIL',prefix) == grant_failures
assert terminals(grants,'PASS',prefix) == cases-grant_failures
head = read('captured-head-retry-bypass.log')
assert terminals(head,'FAIL',prefix) == {prefix+'collector-at-cas'}
assert terminals(head,'PASS',prefix) == cases-{prefix+'collector-at-cas'}
iterator = read('node-iterator-race.log')
assert terminals(iterator,'PASS','TestNodeIteratorBoundedFreshTraversalAndFailures/') == {'TestNodeIteratorBoundedFreshTraversalAndFailures/'+mode for mode in ('normal','deadline','cancel','corrupt','visitor')}
assert '--- FAIL:' not in iterator and 'ok  \tjs-wf/internal/retainedgraph\t1.313s' in iterator
assert 'mode=normal records=131 nodes=259 gets=259 max_pending=10' in iterator
assert 'mode=deadline records=131 nodes=259 gets=260 max_pending=10' in iterator
pins = read('all-853-pins.log.gz')
assert len(terminals(pins,'PASS','TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t13.454s' in pins
native = read('native20-recovery-race.log.gz')
assert '--- FAIL:' not in native and 'ok  \tjs-wf/worker\t25.372s' in native
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
assert len(terminals(native,'PASS','TestGraphContinuationRepairsArchiveBeforeStage/')) == 7
commands = json.loads((base/'commands.json').read_text())
assert len(commands) == 4 and all(row['exit_code'] == 0 for row in commands)
mutants = json.loads((base/'mutant-commands.json').read_text())
assert len(mutants) == 4 and all(row['exit_code'] == 1 and not row['race'] for row in mutants.values())
result = dict(scope='Development controls only; no frozen current-source or original whole-plan acceptance.',
              source_sha256=inputs,commit_controls=15,node_iterator_controls=5,
              required_bypass_failures=dict(record_budget=15,node_budget=11,node_grant=2,refreshed_head=1),
              unchanged_saved_pins=853,native_bounded_restart_replicas=[1,3],
              native_compared_records=4,native_verified_nodes=6,native_record_budget=1,native_node_budget=1,
              private_process_local_verification=True,portable_verified_certificate=False,
              worker_synchronous=True,worker_context_seconds=15,intent_renewal=False,
              actual100000_accepted=False)
(base/'development-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
