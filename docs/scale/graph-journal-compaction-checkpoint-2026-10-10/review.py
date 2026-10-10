"""Verify bound-compaction development evidence, not runtime rollout."""
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

prefix = 'TestGraphCompactionCheckpointBindingRenewalAndResumption/'
modes = ('normal', 'renew', 'renew-drop', 'renew-lost', 'verify', 'every-batch',
         'type', 'id', 'runtime', 'tail', 'application', 'cut', 'limit', 'append', 'reader', 'retire', 'collector')
cases = {prefix + enc + '/' + mode for enc in ('json', 'protobuf-v1') for mode in modes}
malformed = 'TestGraphCompactionCheckpointRejectsNoncanonicalEnvelope/'
bad_modes = ('empty', 'oversize', 'null', 'unknown', 'duplicate', 'alias', 'escaped',
             'leading', 'trailing', 'second-object', 'zero-expiry')
bad_cases = {malformed + mode for mode in bad_modes}
positive = read('restored-45-controls-race.log.gz')
assert terminals(positive, 'PASS', prefix) == cases
assert terminals(positive, 'PASS', malformed) == bad_cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/journal\t8.584s' in positive
assert positive.count('mode=renew-lost renewal_batches=11 verification_batches=10 retained_from=9 readers=0') == 2
expected = {
    'binding-bypass': {'application', 'cut', 'limit'},
    'identity-bypass': {'type', 'id', 'runtime', 'tail'},
    'requested-expiry-bypass': {'renew', 'renew-drop', 'renew-lost'},
}
for name, failed in expected.items():
    raw = read(name + '.log')
    failures = {prefix + enc + '/' + mode for enc in ('json', 'protobuf-v1') for mode in failed}
    assert terminals(raw, 'FAIL', prefix) == failures
    assert terminals(raw, 'PASS', prefix) == cases - failures
    assert terminals(raw, 'PASS', malformed) == bad_cases
raw = read('canonical-envelope-bypass.log')
failures = {malformed + m for m in ('duplicate', 'alias', 'escaped', 'leading', 'trailing')}
assert terminals(raw, 'FAIL', malformed) == failures
assert terminals(raw, 'PASS', malformed) == bad_cases - failures
assert terminals(raw, 'PASS', prefix) == cases
source = (repo / 'journal/graph_compaction_checkpoint.go').read_text()
mutations = {
    'binding-bypass': source.replace('if !stage.MatchesBinding(', 'if false && !stage.MatchesBinding('),
    'identity-bypass': source.replace('v.Type != typ', 'false && v.Type != typ').replace('v.ID != id', 'false && v.ID != id').replace('v.Runtime != runtime', 'false && v.Runtime != runtime').replace('v.Tail != tail', 'false && v.Tail != tail'),
    'requested-expiry-bypass': source.replace('c.tail, stage, c.renewTo}', 'c.tail, stage, nil}'),
    'canonical-envelope-bypass': source.replace('!bytes.Equal(data, canonical)', 'false && !bytes.Equal(data, canonical)'),
}
for name, mutant in mutations.items():
    assert read(name + '.go.txt.gz') == mutant
mutants = json.loads((base / 'mutant-commands.json').read_text())
assert set(mutants) == set(mutations) and all(c['exit_code'] == 1 and not c['race'] for c in mutants.values())
excluded = read('excluded-initial-retirement-fixture.log.gz')
assert terminals(excluded, 'FAIL', prefix) == {prefix + enc + '/retire' for enc in ('json', 'protobuf-v1')}
assert 'journal compare-and-swap lost' in excluded
broad = read('journal-checkpoint-archive-race.log.gz')
assert '--- FAIL:' not in broad and 'ok  \tjs-wf/journal\t62.927s' in broad
assert terminals(broad, 'PASS', prefix) == cases
assert terminals(broad, 'PASS', malformed) == bad_cases
native = read('native-r1-r3-bound-resume-race.log.gz')
assert '--- FAIL:' not in native and 'ok  \tjs-wf/journal\t51.494s' in native
assert terminals(native, 'PASS', 'TestNativeGraphCheckpointArchiveReopenAndCollection/') == {
    'TestNativeGraphCheckpointArchiveReopenAndCollection/R1-domain',
    'TestNativeGraphCheckpointArchiveReopenAndCollection/R3-domain'}
assert native.count('NATIVE_BOUND_COMPACTION renewal_batches=10 verification_batches=8 saved_bytes=7386 old_reader_preserved=true expired_old_intents_swept=true') == 2
assert native.count('ARCHIVE_AUTHORITY_RECOVERED head=') == 4
assert native.count('raw_chunks_after_retirement=0') == 2
worker = read('worker-maintenance-repair-race.log.gz')
assert '--- FAIL:' not in worker and 'ok  \tjs-wf/worker\t6.606s' in worker
assert len(terminals(worker, 'PASS', 'TestGraphContinuationMaintenanceLeaseAndCancellation/')) == 22
assert len(terminals(worker, 'PASS', 'TestGraphContinuationRepairsArchiveBeforeStage/')) == 7
pins = read('all-853-pins.log.gz')
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t4.665s' in pins
assert len(terminals(pins, 'PASS', 'TestPinnedRegressionCorpus/')) == 853
commands = json.loads((base / 'commands.json').read_text())
assert len(commands) == 5 and all(c['exit_code'] == 0 for c in commands)
result = dict(scope='Component development evidence; no durable worker adoption or original whole-plan acceptance.',
              source_sha256=inputs, bound_resume_controls=34, malformed_envelope_controls=11,
              bypass_failures=dict(binding=6, identity=8, requested_expiry=6, canonical_envelope=5),
              unchanged_saved_pins=853, native_domain_replicas=[1, 3], native_peer_restart_cuts_per_case=2,
              native_old_reader_preserved=True, runtime_descriptor_store_durability=False,
              private_verification_progress_resumed=False, journal_explicit_staging_renewal=True,
              worker_persisted_maintenance=False, worker_intent_renewal=False,
              worker_handoff_deadline_seconds=15, bounded_memory_namespace_scan=False,
              actual100000_accepted=False)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
