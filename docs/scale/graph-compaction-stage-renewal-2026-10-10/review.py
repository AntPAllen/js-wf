"""Verify staged-renewal component evidence, not whole-plan qualification."""
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
    data = (base / name).read_bytes()
    return (gzip.decompress(data) if name.endswith('.gz') else data).decode()

def terminals(raw, action, prefix):
    return {n for n in re.findall(r'--- ' + action + r': (\S+)', raw) if n.startswith(prefix)}

prefix = 'TestGraphCompactionStageRenewalPauseResumeAndFences/'
modes = ('normal', 'resume-before', 'resume-after', 'repeated', 'empty', 'complete',
         'cancel', 'drop', 'lost', 'upload-drop', 'upload-lost', 'expired', 'collector', 'append')
cases = {prefix + m for m in modes}
positive = read('restored-16-controls-race.log.gz')
assert terminals(positive, 'PASS', prefix) == cases
assert '--- FAIL:' not in positive and 'ok  \tjs-wf/internal/graphpublication\t2.478s' in positive
assert 'mode=normal prefix=4 examined=42 renewed=8 records=12 nodes=19' in positive
assert 'mode=repeated prefix=6 examined=44 renewed=10 records=12 nodes=19' in positive
forged = 'TestGraphCompactionStageRenewalDoesNotCertifyForgedPrefix/'
forged_cases = {forged + m for m in ('data', 'payload')}
assert terminals(positive, 'PASS', forged) == forged_cases
assert positive.count('commit_rejected=true root_unchanged=true') == 2
for name in ('staging-freeze-bypass', 'checkpoint-freeze-bypass'):
    raw = read(name + '.log')
    assert terminals(raw, 'FAIL', prefix) == cases - {prefix + 'expired'}
    assert terminals(raw, 'PASS', prefix) == {prefix + 'expired'}
raw = read('expiry-adoption-bypass.log')
early = {prefix + m for m in ('expired', 'collector', 'append')}
assert terminals(raw, 'FAIL', prefix) == cases - early
assert terminals(raw, 'PASS', prefix) == early
raw = read('content-comparison-bypass.log')
assert terminals(raw, 'FAIL', forged) == forged_cases
assert raw.count('renewal certified forged prefix') == 2
mutants = json.loads((base / 'mutant-commands.json').read_text())
assert set(mutants) == {'staging-freeze-bypass', 'checkpoint-freeze-bypass', 'expiry-adoption-bypass', 'content-comparison-bypass'}
assert all(c['exit_code'] == 1 and not c['race'] for c in mutants.values())
stage = (repo / 'internal/graphpublication/compaction_stage.go').read_text()
assert read('staging-freeze-bypass.go.txt.gz') == stage.replace('if s.renewal != nil {\n\t\treturn PreparedCompaction{}, false, ErrConflict\n\t}', '')
assert read('checkpoint-freeze-bypass.go.txt.gz') == stage.replace('if s.renewal != nil {\n\t\treturn nil, ErrConflict\n\t}', '')
renewal = (repo / 'internal/graphpublication/compaction_stage_renewal.go').read_text()
assert read('expiry-adoption-bypass.go.txt') == renewal.replace('\tr.stage.prepared.expires = r.operation.expires\n', '')
commit = (repo / 'internal/graphpublication/compaction_commit.go').read_text()
assert read('content-comparison-bypass.go.txt') == commit.replace(
    'if !bytes.Equal(record.Data, next.Data) || len(record.Blobs) != len(next.Blobs) {',
    'if false && (!bytes.Equal(record.Data, next.Data) || len(record.Blobs) != len(next.Blobs)) {').replace(
    'if link.Hash != record.Blobs[n].Hash {', 'if false && link.Hash != record.Blobs[n].Hash {')
native = read('compaction-and-native-race.log.gz')
assert '--- FAIL:' not in native and 'ok  \tjs-wf/internal/graphpublication\t58.020s' in native
assert terminals(native, 'PASS', prefix) == cases
for name in ('TestNativeGraphCompactionStageStoreRestart/', 'TestNativeGraphCompactionStageRenewalStoreRestart/'):
    assert terminals(native, 'PASS', name) == {name + 'R1', name + 'R3'}
for replicas in (1, 3):
    assert f'NATIVE_STAGE_RENEWAL replicas={replicas} checkpoint_next=1 examined=13 renewed=2 scope_budget=1 root_unchanged_after_old_expiry=true' in native
    assert native.count(f'replicas={replicas} checkpoint_next=1 records=4 archive=2 live=2 original_objects_deleted=11') == 2
    assert native.count(f'NATIVE_COMPACTION_COMMIT replicas={replicas} compared_records=4 verified_nodes=6 record_budget=1 node_budget=1') == 2
pins = read('all-853-pins.log.gz')
assert len(terminals(pins, 'PASS', 'TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t9.599s' in pins
commands = json.loads((base / 'commands.json').read_text())
assert len(commands) == 3 and all(c['exit_code'] == 0 for c in commands)
result = dict(scope='Component development evidence; runtime adoption and original whole-plan gates remain open.',
              source_sha256=inputs, stage_renewal_controls=14, renewed_forged_prefix_controls=2,
              bypass_failures=dict(staging_freeze=13, checkpoint_freeze=13, expiry_adoption=11, content_comparison=2),
              unchanged_saved_pins=853, native_existing_and_renewed_replicas=[1, 3],
              native_examined_scopes=13, native_renewed_scopes=2, native_scope_budget=1,
              renewed_metadata_second_peer_restart=False, vm_power_storage_loss_qualified=False,
              durable_runtime_binding=False, bounded_memory_namespace_scan=False,
              runtime_intent_renewal=False, runtime_handoff_deadline_seconds=15,
              actual100000_accepted=False)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
