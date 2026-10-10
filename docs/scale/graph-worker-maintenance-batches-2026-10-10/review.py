"""Verify scoped worker-maintenance development evidence."""
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

def terminals(raw, outcome, prefix):
    return {n for n in re.findall(r'--- ' + outcome + r': (\S+)', raw) if n.startswith(prefix)}

prefix = 'TestGraphContinuationMaintenanceLeaseAndCancellation/'
modes = ('normal', 'owner-loss-pointer', 'owner-loss-confirm', 'owner-loss-stage',
         'owner-loss-verify', 'owner-loss-nodes', 'cancel-pointer', 'cancel-confirm',
         'cancel-stage', 'cancel-verify', 'cancel-nodes')
cases = {prefix + enc + '/' + mode for enc in ('json', 'protobuf-v1') for mode in modes}
directed = read('directed-22-race.log.gz')
assert terminals(directed, 'PASS', prefix) == cases
assert '--- FAIL:' not in directed and 'ok  \tjs-wf/worker\t8.573s' in directed
assert directed.count('mode=normal pointer_batches=6 confirm_batches=1 stage_batches=6 verify_batches=10 readers=0 initial_calls=1 stage_calls=1 retained_from=9') == 2
renew = read('lease-renew-bypass.log')
loss = {n for n in cases if '/owner-loss-' in n}
assert terminals(renew, 'FAIL', prefix) == loss
assert terminals(renew, 'PASS', prefix) == cases - loss
assert renew.count('partial verification published handoff') == 2
assert renew.count('abandoned archive published relocation') == 8
assert terminals(read('budget-bypass.log'), 'FAIL', prefix) == cases
mutants = json.loads((base / 'mutant-commands.json').read_text())
assert set(mutants) == {'lease-renew-bypass', 'budget-bypass'}
assert all(row['exit_code'] == 1 and not row['race'] for row in mutants.values())
source = (repo / 'worker/continuation_maintenance.go').read_text()
assert read('lease-renew-bypass.go.txt') == source.replace(
    '_, timing, renewErr := ops.renew(renewCtx, owner, 0)',
    'timing, renewErr := lease.RenewalTiming{}, error(nil)\n\t\t_ = renewCtx')
assert read('budget-bypass.go.txt') == source.replace('budget := w.continuationVerifyBatch', 'budget := uint64(128)')
initial = read('initial-pins-release-regression.log.gz')
assert terminals(initial, 'FAIL', 'TestPinnedRegressionCorpus/') == {
    'TestPinnedRegressionCorpus/graph-compaction-release-drop.json',
    'TestPinnedRegressionCorpus/graph-compaction-release-unconfirmed.json'}
pins = read('all-853-pins.log.gz')
assert len(terminals(pins, 'PASS', 'TestPinnedRegressionCorpus/')) == 853
assert '--- FAIL:' not in pins and 'ok  \tjs-wf/sim\t11.560s' in pins
native = read('native20-and-controls-race.log.gz')
assert '--- FAIL:' not in native and 'ok  \tjs-wf/worker\t30.542s' in native
assert terminals(native, 'PASS', prefix) == cases
assert len(terminals(native, 'PASS', 'TestGraphContinuationRepairsArchiveBeforeStage/')) == 7
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
journal = read('journal-checkpoint-archive-race.log.gz')
assert '--- FAIL:' not in journal and re.search(r'^ok  \tjs-wf/journal\t[0-9.]+s$', journal, re.M)
commands = json.loads((base / 'commands.json').read_text())
assert len(commands) == 4 and all(row['exit_code'] == 0 for row in commands)
result = dict(scope='Component development evidence; original whole-plan gates remain open.',
              source_sha256=inputs, maintenance_controls=22, lease_bypass_failures=10,
              budget_bypass_failures=22, unchanged_saved_pins=853,
              native_budget=20, handoff_repair_controls=7,
              worker_record_budget=128, worker_node_budget=256,
              outer_handoff_deadline_seconds=15, intent_renewal=False,
              durable_runtime_progress=False, actual100000_accepted=False)
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
