"""Review component results; this is not frozen full-suite qualification."""
import hashlib
import json
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
checks = {
    'final-cli-normal.jsonl': {'TestWorkerGraphAdmissionRejectsIncompatibleCLI', 'TestWorkerRunnerCanonicalGraphRepair', 'TestWorkerRunnerCompletesWorkflowAndServesMetrics', 'TestWorkerRunnerUsesIndependentClockForNativeAndFallbackTimers'},
    'final-cli-race.jsonl': {'TestWorkerGraphAdmissionRejectsIncompatibleCLI', 'TestWorkerRunnerCanonicalGraphRepair', 'TestWorkerRunnerCompletesWorkflowAndServesMetrics', 'TestWorkerRunnerUsesIndependentClockForNativeAndFallbackTimers'},
    'final-model-normal.jsonl': None,
    'final-model-race.jsonl': None,
    'final-journal-race.jsonl': None,
    'family-normal1000.jsonl': {'TestSeededGraphReconcileReplay'},
    'start-family-normal1000.jsonl': {'TestSeededGraphStartRepairReplay'},
    'final-all-sim-race.jsonl': {'TestPinnedRegressionCorpus', 'TestSeededGraphStartRepairReplay', 'TestSeededGraphReconcileReplay'},
    'final-pins-after-start-normal.jsonl': {'TestPinnedRegressionCorpus'},
    'start-repair-race.jsonl': {'TestCanonicalBoundStartRepairRequiresExactSourceAndGeneration', 'TestCanonicalBoundStartRepairDoesNotDeduplicateRecovery'},
}
results = []
pins = {p.name for p in (repo/'sim/testdata/regressions').glob('*.json')}
assert len(pins) == 748
for filename, expected in checks.items():
    data = (base/filename).read_bytes()
    rows = [json.loads(line) for line in data.splitlines()]
    assert rows[-1]['Action'] == 'pass' and not rows[-1].get('Test'), filename
    assert not any(row['Action'] == 'fail' or 'WARNING: DATA RACE' in row.get('Output', '') for row in rows), filename
    passes = {row['Test'] for row in rows if row['Action'] == 'pass' and row.get('Test')}
    top = {name for name in passes if '/' not in name}
    if expected is not None:
        assert top == expected, (filename, top)
    output = ''.join(row.get('Output', '') for row in rows)
    for name in top:
        if name.startswith('TestSeeded'):
            assert f'TIER1_SEEDS test={name} first=1 last=1000 completed=1000 requested=1000' in output
    if filename.startswith('final-cli'):
        group = 'TestWorkerRunnerCanonicalGraphRepair'
        assert {group+'/R1', group+'/R3Domain'}.issubset(passes)
        assert output.count('native timer completion, two terminal restorations') == 2
        assert 'wrong_prefix_requests=0' in output
        assert {'TestWorkerRunnerCompletesWorkflowAndServesMetrics/'+mode for mode in ['static', 'kv', 'auto']}.issubset(passes)
        assert {'TestWorkerRunnerUsesIndependentClockForNativeAndFallbackTimers/'+mode for mode in ['native', 'fallback']}.issubset(passes)
    if filename.startswith('final-model'):
        assert {'TestGraphReconcileRetriesPublishedUnboundStart', 'TestGraphReconcileDefersHistoryPinsWhileDeliveryOwnsLease'}.issubset(top)
        for group in ['TestGraphReconcileRetriesPublishedUnboundStart', 'TestGraphReconcileDefersHistoryPinsWhileDeliveryOwnsLease']:
            assert {group+'/timer', group+'/suspended'}.issubset(passes)
    if filename == 'final-journal-race.jsonl':
        assert 'TestGraphEmptyHistoryInspectionDoesNotPublishReader' in top and len(top) == 12
    if 'TestPinnedRegressionCorpus' in top:
        assert {name.split('/', 1)[1] for name in passes if name.startswith('TestPinnedRegressionCorpus/')} == pins
    results.append(dict(log=filename, sha256=hashlib.sha256(data).hexdigest(), seconds=rows[-1]['Elapsed'], groups=sorted(top)))

lineages = [json.loads((base/'pin-lineage.json').read_text())] + json.loads((base/'start-pin-lineage.json').read_text())
assert len(lineages) == 10
for entry in lineages:
    old = subprocess.check_output(['git', 'show', entry['prior_source']+':'+entry['path']], cwd=repo)
    new = (repo/entry['path']).read_bytes()
    assert hashlib.sha256(old).hexdigest() == entry['prior_sha256']
    assert hashlib.sha256(new).hexdigest() == entry['current_sha256']
    a, b = json.loads(old), json.loads(new)
    assert {k:v for k,v in a.items() if k != 'transport'} == {k:v for k,v in b.items() if k != 'transport'}
changed = {entry['path'] for entry in lineages}
for path in (repo/'sim/testdata/regressions').glob('*.json'):
    rel = str(path.relative_to(repo))
    if rel not in changed:
        assert path.read_bytes() == subprocess.check_output(['git', 'show', '712a8d7:'+rel], cwd=repo), rel

negative = [json.loads(line) for line in (base/'counterfactual.jsonl').read_text().splitlines()]
assert negative[-1]['Action'] == 'fail'
for kind in ['timer', 'suspended']:
    assert any(row['Action'] == 'fail' and row.get('Test') == 'TestGraphReconcileRetriesPublishedUnboundStart/'+kind for row in negative)
negative = [json.loads(line) for line in (base/'start-repair-dedup-negative.jsonl').read_text().splitlines()]
assert negative[-1]['Action'] == 'fail'
assert any('acknowledged repair was deduplicated' in row.get('Output','') for row in negative)
for name in ['normal.jsonl', 'domain-diagnostic.jsonl', 'retry-only-cli-normal.jsonl', 'empty-cli-race-without-lease-hint.jsonl', 'timer-cli-before-dedup-normal.jsonl', 'timer-cli-before-dedup-race.jsonl', 'pins-before-migration.jsonl', 'pins-after-dedup.jsonl']:
    rows = [json.loads(line) for line in (base/name).read_text().splitlines()]
    assert rows[-1]['Action'] == 'fail', name

before = json.loads((base/'final-inputs-before.json').read_text())
after = {name:hashlib.sha256((repo/name).read_bytes()).hexdigest() for name in before['files']}
changed_inputs = {name for name, digest in before['files'].items() if after[name] != digest}
assert changed_inputs == {entry['path'] for entry in lineages[1:]}
(base/'final-inputs-after.json').write_text(json.dumps(dict(scope=before['scope'], files=after), indent=2)+'\n')
report = dict(verdict='PASS for final component controls', scope='Development evidence; complete current frozen suite and wider original gates remain separate', results=results, selected_go_config_inputs_unchanged=True, corpus_changes_during_final_cli_run='Nine trace migrations only; CLI does not consume trace files', migrated_pins=10, unchanged_pins=738, total_pins=748, source_observation_before_compilation=False, preserved_failed_native_runs=6, counterfactuals='Expected FAIL for original stale classification and deduplicated recovery')
(base/'review.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report))
