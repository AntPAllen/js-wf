#!/usr/bin/env python3
"""Check focused completed seed counts, exact-replay gates and six replay pins."""
import hashlib
import json
from pathlib import Path
import re

base = Path(__file__).resolve().parent
repo = base.parents[2]
results = json.loads((base / 'results.json').read_text())
for name in ('normal', 'race'):
    assert results[name]['actual_child_exit'] == 0
    data = (base / (name + '.log')).read_text()
    assert re.search(r'TIER1_SEEDS test=TestSeededGraphFallbackNamespaceReplay first=1 last=1000 completed=1000 requested=1000', data)
    assert '--- PASS: TestSeededGraphFallbackNamespaceReplay' in data
    assert '24 fallback namespace mode/order/batch combinations' in data
    assert not any(term in data for term in ('--- FAIL:', 'WARNING: DATA RACE', 'panic:'))
    tuples = re.findall(r'(?:healthy|read_unknown|publish_drop|publish_ack|delete_drop|delete_ack)/(?:owned_first|foreign_first)/[12]:(\d+)', data)
    assert len(tuples) == 24 and sum(map(int, tuples)) == 1000
pins = json.loads((base / 'pins.json').read_text())
assert len(pins) == 6
for pin in pins.values():
    path = repo / pin['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == pin['sha256']
    trace = json.loads(path.read_text())
    assert trace['workload'] == 'graph_fallback_namespace' and trace['seed'] == pin['seed']
    for name in ('pins-normal', 'pins-race'):
        assert results[name]['actual_child_exit'] == 0
        assert '--- PASS: TestPinnedRegressionCorpus/' + path.name in (base / (name + '.log')).read_text()
for path, digest in json.loads((base / 'review-inputs.json').read_text()).items():
    assert hashlib.sha256((repo / path).read_bytes()).hexdigest() == digest
report = {'focused_normal_and_race_seed_bodies': 1000, 'declared_tuples': 24,
          'verified_new_pins': 6, 'review_sources_unchanged': True,
          'scope': 'Focused workload/pin checks; no full current-suite or native adoption verdict.'}
(base / 'executed-review.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
