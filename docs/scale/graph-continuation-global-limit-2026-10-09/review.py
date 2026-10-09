"""Review the selected native limit checks and suffix-budget negative control."""
import hashlib
import json
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
positive = json.loads((base / 'native-command.json').read_text())
negative = json.loads((base / 'negative-command.json').read_text())
log = (base / 'native-race.log').read_text()
bad = (base / 'negative-suffix-limit-race.log').read_text()
assert positive['exit'] == 0 and negative['exit'] == 1
assert '-race' in positive['command'] and '-race' in negative['command']
assert positive['command'][positive['command'].index('-run') + 1] == '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$'
assert negative['command'][negative['command'].index('-run') + 1] == '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true$'
assert hashlib.sha256((repo / 'worker/graph_continuation_limit_test.go').read_bytes()).hexdigest() == positive['test_source_sha256']
original = subprocess.check_output(['git', 'show', positive['head'] + ':worker/worker.go'], cwd=repo)
assert hashlib.sha256(original).hexdigest() == negative['source_sha256']
assert (repo / 'worker/worker.go').read_bytes() == original
mutated = original.decode()
for old, new in negative['mutations'].items():
    assert mutated.count(old) == 1
    mutated = mutated.replace(old, new)
assert hashlib.sha256(mutated.encode()).hexdigest() == negative['replacement_sha256']
for replicas in [1, 3]:
    for archive in ['false', 'true']:
        assert '--- PASS: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R%d/archive=%s' % (replicas, archive) in log
        marker = 'GRAPH_CONTINUATION_LIMIT budget=16 entries=16 archive=%s checkpoints=2 effects=0 rejected_request=must_not_run terminal_slot=15 prefix_stage_calls=1/1' % archive
        assert log.count(marker) == 2
assert 'limit reset across checkpoints <nil> 1 map[finish:2 initial:1 middle:1]' in bad
assert '--- FAIL: TestNativeGraphContinuationGlobalLimitAndTerminalSlot/R1/archive=true' in bad
assert all(marker not in log + bad for marker in ['WARNING: DATA RACE', 'panic:', 'context deadline exceeded'])
result = dict(production_revision=positive['head'], test_sha256=positive['test_source_sha256'],
              positive_native_cases=4, private_fixture_budget=16, production_default_budget=100000,
              checkpoints_per_case=2, forbidden_effects_per_positive_case=0,
              suffix_guard_negative_detects_one_forbidden_effect=True,
              scope='Direct leased deliveries on native R1/R3 domain storage with and without actual compaction. No SIGKILL, actual 100000-entry boundary, complete current suite or public admission claim.')
(base / 'executed-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
