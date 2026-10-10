"""Verify development evidence; this does not qualify runtime renewal."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
inputs = json.loads((base / 'source-inputs.json').read_text())
assert inputs == {p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in inputs}

def read(name):
    raw = (base / name).read_bytes()
    return (gzip.decompress(raw) if name.endswith('.gz') else raw).decode()

def terminals(raw, action, prefix):
    return {n for n in re.findall(r'--- ' + action + r': (\S+)', raw) if n.startswith(prefix)}

prefix = 'TestGraphCompactionIntentRenewalScopesAndFences/'
modes = ('normal', 'inherited', 'drop', 'lost', 'cancel', 'expired', 'expiry-read',
         'expiry-after-cas', 'append', 'append-during-cas', 'reader', 'collector',
         'collector-during-cas', 'revoked', 'wrong-expiry', 'input-ownership', 'result-ownership')
cases = {prefix + m for m in modes}
positive = read('compaction-and-native-race.log.gz')
assert terminals(positive, 'PASS', prefix) == cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/internal/graphpublication\t35.385s' in positive
assert '--- PASS: TestGraphCompactionUnrenewedIntermediateScopesFenceSource ' in positive
assert 'mode=normal examined=55 renewed=21 scope_budget=3 root_unchanged_after_old_expiry=true' in positive
assert 'mode=inherited examined=74 renewed=18 scope_budget=3 root_unchanged_after_old_expiry=true' in positive
assert terminals(positive, 'PASS', 'TestNativeGraphCompactionStageStoreRestart/') == {
    'TestNativeGraphCompactionStageStoreRestart/R1', 'TestNativeGraphCompactionStageStoreRestart/R3'}
for replicas in (1, 3):
    assert f'NATIVE_COMPACTION_COMMIT replicas={replicas} compared_records=4 verified_nodes=6 record_budget=1 node_budget=1' in positive
expected = {
    'expiry-bypass': {'expired', 'expiry-read', 'expiry-after-cas'},
    'source-bypass': {'append', 'append-during-cas', 'reader'},
    'intermediate-bypass': {'normal', 'inherited', 'drop', 'lost', 'cancel', 'input-ownership', 'result-ownership'},
}
for name, failed in expected.items():
    raw = read(name + '.log')
    failures = {prefix + m for m in failed}
    assert terminals(raw, 'FAIL', prefix) == failures
    assert terminals(raw, 'PASS', prefix) == cases - failures
initial = read('initial-intermediate-bypass-passed.log')
assert terminals(initial, 'PASS', prefix) == cases and '--- FAIL:' not in initial
excluded = read('excluded-initial-fixture-id-failure.log.gz')
assert 'invalid publication ID' in excluded
assert terminals(excluded, 'FAIL', prefix) == {prefix + 'collector-during-cas'}
pins = read('all-853-pins.log.gz')
assert len(terminals(pins, 'PASS', 'TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t6.840s' in pins
commands = json.loads((base / 'commands.json').read_text())
assert len(commands) == 2 and all(c['exit_code'] == 0 for c in commands)
mutants = json.loads((base / 'mutant-commands.json').read_text())
assert set(mutants) == set(expected) and all(c['exit_code'] == 1 and not c['race'] for c in mutants.values())
source = (repo / 'internal/graphpublication/compaction_renewal.go').read_text()
assert read('expiry-bypass.go.txt') == source.replace(
    'if current.IsZero() || !current.Before(r.prepared.expires) {',
    'if false && (current.IsZero() || !current.Before(r.prepared.expires)) {')
assert read('source-bypass.go.txt') == source.replace(
    'if root.Head != r.prepared.expected || !reflect.DeepEqual(root, r.prepared.base) {',
    'if false && (root.Head != r.prepared.expected || !reflect.DeepEqual(root, r.prepared.base)) {')
result = dict(scope='Component development evidence only; runtime renewal remains unintegrated.',
              source_sha256=inputs, renewal_controls=17, expired_baseline_controls=1,
              bypass_failures={k: len(v) for k, v in expected.items()}, unchanged_saved_pins=853,
              native_existing_restart_replicas=[1, 3], native_renewal_qualified=False,
              scope_budget=3, normal_examined=55, normal_renewed=21,
              complete_namespace_keys_held_in_memory=True,
              runtime_intent_renewal=False, runtime_handoff_deadline_seconds=15,
              durable_runtime_progress=False, actual100000_accepted=False)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
