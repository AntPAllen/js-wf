"""Verify private verification-renewal component evidence."""
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

prefix = 'TestGraphCompactionVerificationRenewalPrivateProgressAndFences/'
modes = ('records', 'nodes', 'repeated', 'verifier-port', 'wrong-stage', 'cancel',
         'expired', 'append', 'collector', 'collector-during', 'drop', 'lost')
cases = {prefix + m for m in modes}
positive = read('restored-direct-race.log.gz')
assert terminals(positive, 'PASS', prefix) == cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/internal/graphpublication\t2.283s' in positive
assert 'mode=nodes preserved_records=12 preserved_nodes=2 records=12 nodes=19' in positive
expected = {
    'verifier-freeze-bypass': set(modes) - {'wrong-stage', 'expired'},
    'verifier-expiry-bypass': {'records', 'nodes', 'repeated', 'verifier-port', 'cancel'},
    'verifier-authority-bypass': {'verifier-port'},
}
for name, failed in expected.items():
    raw = read(name + '.log')
    failures = {prefix + m for m in failed}
    assert terminals(raw, 'FAIL', prefix) == failures
    assert terminals(raw, 'PASS', prefix) == cases - failures
commit = (repo / 'internal/graphpublication/compaction_commit.go').read_text()
renewal = (repo / 'internal/graphpublication/compaction_stage_renewal.go').read_text()
assert read('verifier-freeze-bypass.go.txt.gz') == commit.replace('if c.renewal != nil {\n\t\treturn Root{}, false, ErrConflict\n\t}', '')
assert read('verifier-expiry-bypass.go.txt.gz') == renewal.replace('\t\tr.commit.prepared.expires = r.operation.expires\n', '')
assert read('verifier-authority-bypass.go.txt.gz') == renewal.replace(
    'operation, err := c.protocol.BeginCompactionIntentRenewal(ctx, c.prepared, now, expires)',
    'operation, err := stage.protocol.BeginCompactionIntentRenewal(ctx, c.prepared, now, expires)')
mutants = json.loads((base / 'mutant-commands.json').read_text())
assert set(mutants) == set(expected) and all(c['exit_code'] == 1 and not c['race'] for c in mutants.values())
graph = read('graph-compaction-native-race.log.gz')
assert '--- FAIL:' not in graph and 'ok  \tjs-wf/internal/graphpublication\t39.297s' in graph
assert terminals(graph, 'PASS', prefix) == cases
journal = read('journal-bound-native-race.log.gz')
assert '--- FAIL:' not in journal and 'ok  \tjs-wf/journal\t70.571s' in journal
assert len(terminals(journal, 'PASS', 'TestGraphCompactionCheckpointBindingRenewalAndResumption/')) == 40
assert len(terminals(journal, 'PASS', 'TestGraphCompactionCheckpointRejectsNoncanonicalEnvelope/')) == 11
assert journal.count('mode=verify-renew-live renewal_batches=18 verification_batches=9 retained_from=9 readers=0') == 2
for mode in ('verify-renew-resume', 'verify-renew-lost'):
    assert journal.count(f'mode={mode} renewal_batches=18 verification_batches=10 retained_from=9 readers=0') == 2
assert terminals(journal, 'PASS', 'TestNativeGraphCheckpointArchiveReopenAndCollection/') == {
    'TestNativeGraphCheckpointArchiveReopenAndCollection/R1-domain',
    'TestNativeGraphCheckpointArchiveReopenAndCollection/R3-domain'}
assert journal.count('NATIVE_BOUND_COMPACTION renewal_batches=25 verification_batches=8 saved_bytes=7386 old_reader_preserved=true expired_old_intents_swept=true') == 2
assert journal.count('raw_chunks_after_retirement=0') == 2
pins = read('all-853-pins.log.gz')
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t5.641s' in pins
assert len(terminals(pins, 'PASS', 'TestPinnedRegressionCorpus/')) == 853
commands = json.loads((base / 'commands.json').read_text())
assert len(commands) == 4 and all(c['exit_code'] == 0 for c in commands)
result = dict(scope='Component development evidence; original whole-plan and worker adoption gates remain open.',
              source_sha256=inputs, direct_controls=12, journal_controls=40, malformed_controls=11,
              bypass_failures={k: len(v) for k, v in expected.items()}, unchanged_saved_pins=853,
              native_domain_replicas=[1, 3], native_renewal_batches=25, native_verification_batches=8,
              private_progress_preserved_during_successful_renewal=True,
              serialized_verified_prefix=False, durable_worker_maintenance=False,
              worker_intent_renewal=False, worker_handoff_deadline_seconds=15,
              bounded_memory_namespace_scan=False, actual100000_accepted=False)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
