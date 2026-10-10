"""Verify retained development controls; not frozen qualification."""
import gzip
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
prefix = 'TestGraphContinuationRepairsArchiveBeforeStage/'
healthy = {prefix + f'archive={archive}/healthy' for archive in ('false', 'true')}
cuts = {prefix + f'archive={archive}/{cut}' for archive in ('false', 'true') for cut in ('pointer-cut', 'suspension-cut')}
cuts.add(prefix + 'archive=true/archive-cut')
def terminals(raw, action):
    return set(re.findall(r'--- ' + action + r': (\S+)', raw)) & (healthy | cuts)
positive = (base / 'recovery-race.log').read_text()
negative = (base / 'recovery-bypass.log').read_text()
assert terminals(positive, 'PASS') == healthy | cuts
assert not terminals(positive, 'FAIL')
assert terminals(negative, 'FAIL') == cuts
assert terminals(negative, 'PASS') == healthy
assert negative.count('stage entered before handoff repair 1 <nil>') == 5
assert 'original_receipts_physically_reclaimed=19 before_stage=true' in positive
assert 'ok  \tjs-wf/worker\t4.163s' in positive
original = (base / 'original-archive-failure.log').read_text()
assert 'stage entered before archive repair 1 <nil>' in original
with gzip.open(base / 'all-853-pins.log.gz', 'rt') as file:
    pins = file.read()
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/', pins)) == 853
assert 'ok  \tjs-wf/sim\t18.008s' in pins
with gzip.open(base / 'checkpoint-frame-race.log.gz', 'rt') as file:
    journal = file.read()
assert 'ok  \tjs-wf/journal\t102.792s' in journal
assert '--- FAIL:' not in journal
native = (base / 'native20-race.log').read_text()
assert 'budget=20 entries=20 archive=true checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=19' in native
assert 'ok  \tjs-wf/worker\t41.065s' in native
inputs = ['journal/graph_checkpoint.go', 'worker/worker.go', 'worker/graph_continuation_export_test.go', 'worker/graph_continuation_recovery_test.go']
source = {name: hashlib.sha256((repo / name).read_bytes()).hexdigest() for name in inputs}
assert source == json.loads((base / 'source-inputs.json').read_text()), 'Development source changed; these logs cannot qualify it.'
assert 'if graph != nil && graph.checkpoint != nil && graph.checkpoint.HandoffPending {' in (repo / 'worker/worker.go').read_text()
result = dict(scope='Directed unfrozen development evidence only; original actual100000, native cut campaigns and deployment gates remain open.',
              source_sha256=source, healthy_controls=2, repaired_cut_controls=5, required_bypass_failures=5,
              original_receipts_physically_reclaimed_before_stage=19, unchanged_saved_pins=853,
              bounded_native=dict(budget=20, checkpoints=2, terminal_slot=19, effects=0))
(base / 'development-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
