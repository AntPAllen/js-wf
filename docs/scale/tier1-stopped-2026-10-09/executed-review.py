"""Review stopped campaigns without manufacturing a supervisor completion."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

repo = Path(__file__).resolve().parents[3]
base = Path(__file__).resolve().parent
campaigns = [
    ('race146', '/home/exedev/js-wf-tier1-full146-race1000-20261008', '/home/exedev/js-wf-signal-combined-qualification', 'FAIL'),
    ('normal148', '/home/exedev/js-wf-tier1-full148-cycles-normal1000-20261008', '/home/exedev/js-wf-catalog-cycles-qualification', 'INTERRUPTED'),
]
reports = []
for label, retained, checkout, verdict in campaigns:
    root, source_root = Path(retained), Path(checkout)
    dest = base / label
    dest.mkdir(exist_ok=True)
    before = json.loads((root / 'source-before.json').read_text())
    for path, digest in before['files'].items():
        assert hashlib.sha256(subprocess.check_output(['git', 'show', before['revision'] + ':' + path], cwd=repo)).hexdigest() == digest, path
        assert hashlib.sha256((source_root / path).read_bytes()).hexdigest() == digest, path
    binary = json.loads((root / 'binary.json').read_text())
    assert hashlib.sha256((root / 'sim.test').read_bytes()).hexdigest() == binary['binary_sha256']
    assert binary['race_instrumented'] == (label == 'race146')
    assert not (root / 'source-after.json').exists(), 'Reassess available supervisor evidence'
    files = [p for p in root.iterdir() if p.is_file() and p.name != 'sim.test']
    for path in files:
        shutil.copyfile(path, dest / path.name)
    data = (root / 'tier1-events.jsonl').read_bytes()
    rows = [json.loads(line) for line in data.splitlines()]
    output = ''.join(row.get('Output', '') for row in rows)
    failures = [row['Test'] for row in rows if row['Action'] == 'fail' and row.get('Test')]
    errors = [row['Output'].strip() for row in rows if 'FAULT_SEED=' in row.get('Output', '')]
    complete = [row['Test'] for row in rows if 'TIER1_SEEDS ' in row.get('Output', '') and 'completed=1000 requested=1000' in row['Output']]
    if label == 'race146':
        assert rows[-1]['Action'] == 'fail' and not rows[-1].get('Test')
        assert set(failures) == {'TestSeededGraphSignalRuntimeCombinedReplay', 'TestSeededGraphSignalRuntimeReplay'}
        assert len(errors) == 2 and 'FAULT_SEED=645' in errors[0] and 'FAULT_SEED=971' in errors[1]
        assert all('worker failed to suspend: context deadline exceeded' in error for error in errors)
        assert len(complete) == 144
        assert 'panic: test timed out' not in output and 'WARNING: DATA RACE' not in output
        trace = json.loads((root / 'failure-trace.json').read_text())
        assert trace['seed'] == 971
    else:
        assert not any(row['Action'] in ('pass', 'fail', 'skip') and not row.get('Test') for row in rows)
        assert rows[-1]['Test'] == 'TestSeededGraphSignalRuntimeCombinedReplay'
        assert len(complete) == 39
        assert not (root / 'tier1-time.txt').read_text()
    report = dict(campaign=label, source=before['revision'], verdict=verdict,
                  selected_inputs=len(before['files']), selected_inputs_match_git_and_observed_checkout=True,
                  observed_at='2026-10-09', supervisor_source_after_missing=True,
                  binary_sha256=binary['binary_sha256'], binary_retained_unchanged=True,
                  events_sha256=hashlib.sha256(data).hexdigest(), complete_seeded_families=len(complete),
                  failures=failures, error_diagnostics=errors, last_event=rows[-1],
                  qualification_accepted=False,
                  retained_files_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
    (dest / 'review.json').write_text(json.dumps(report, indent=2) + '\n')
    reports.append(report)
print(json.dumps(reports, indent=2))
