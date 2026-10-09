#!/usr/bin/env python3
"""Check executed component output; never substitutes for broader qualification."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
log = (root / 'final-race.log').read_text()
assert '--- FAIL:' not in log and '--- SKIP:' not in log, 'failed or skipped check'
assert 'ok  \tjs-wf/visibility\t' in log
assert 'ok  \tjs-wf/cmd/wf\t' in log
required = ['TestGraphVisibilityConfiguration', 'TestGraphOperatorRejectsPartialOrLegacyOnlySelection']
for backend in ['', 'Postgres']:
    model = f'TestGraph{backend}VisibilityCanonicalRowsAndUncertainty'
    required.append(model)
    required.extend(f'{model}/{mode}' for mode in ['ordinary', 'catalog-unknown', 'source-forged', 'lease-held', 'lease-unknown', 'retired'])
    native = f'TestNativeCanonicalGraph{backend}OperatorCommands'
    required.append(native)
    required.extend(f'{native}/{mode}' for mode in ['R1', 'R3Domain'])
for name in required:
    assert f'--- PASS: {name} (' in log, f'missing executed PASS: {name}'
assert log.count('graph project subprocess joined; legacy journal requests zero and domain API verified') == 4
repo = root.parents[2]
for line in (root / 'source-observed-during-final-run.sha256').read_text().splitlines():
    digest, path = line.split(None, 1)
    assert hashlib.sha256((repo / path.strip()).read_bytes()).hexdigest() == digest, f'changed source: {path}'
print(json.dumps({'scope':'development components; source observed during run, not frozen qualification', 'required_passes':required, 'log_sha256':hashlib.sha256(log.encode()).hexdigest()}, indent=2))
