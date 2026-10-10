"""Independently review a closed stage; never accept a live stage or pipeline."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('stage', choices=('native', 'normal1000', 'race1000'))
args = parser.parse_args()
base = Path(__file__).resolve().parent
repo = base.parents[2]
state = json.loads((base / 'pipeline.json').read_text())
root = Path(state['root'])
checkout = Path(state['working_directory'])
source = 'd504a337a8b9102a0b32673d457939c4b82fcc8f'
assert state['source'] == source

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', source], cwd=repo, text=True).splitlines()
required = {n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')}
before = json.loads((root / 'source-before.json').read_text())
assert before['source'] == source and set(before['files']) == required
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    for name, expected in before['files'].items():
        process.stdin.write((source + ':' + name + '\n').encode())
        process.stdin.flush()
        header = process.stdout.readline().split()
        assert len(header) == 3 and header[1] == b'blob', name
        content = process.stdout.read(int(header[2]))
        assert process.stdout.read(1) == b'\n'
        assert hashlib.sha256(content).hexdigest() == expected == digest(checkout / name), name
finally:
    process.stdin.close()
    assert process.wait() == 0

review = dict(stage=args.stage, source=source, observed=datetime.datetime.now(datetime.timezone.utc).isoformat(), repository_inputs_git_verified=len(required), pipeline_accepted=False)
if args.stage == 'native':
    row = next(r for r in state['commands'] if r['command'][0] == str(root / 'native/worker-race.test'))
    assert row['actual_exit_code'] == 0 and row['finished']
    assert state['native_compile_exit'] == 0 and state['native_exit'] == 0
    assert row['command'][1:] == ['-test.run=^TestNativeGraphContinuationFailedChildPromise$', '-test.count=1', '-test.timeout=10m', '-test.v=true']
    artifact = root / 'native'
    binary = json.loads((artifact / 'binary.json').read_text())
    assert '-race=true' in binary['build_info']
    assert digest(artifact / 'worker-race.test') == binary['sha256']
    log = (artifact / 'race.log').read_text()
    assert 'WARNING: DATA RACE' not in log and '--- FAIL:' not in log
    expected = {f'{domain}/archive={archive}/buffered={buffered}' for domain in ('R1', 'R3Domain') for archive in ('false', 'true') for buffered in ('false', 'true')}
    passed = re.findall(r'--- PASS: TestNativeGraphContinuationFailedChildPromise/(\S+) \(([0-9.]+)s\)', log)
    assert len(passed) == 8 and {name for name, _ in passed} == expected
    assert len(re.findall(r'^--- PASS: TestNativeGraphContinuationFailedChildPromise \(', log, re.M)) == 1
    assert log.rstrip().endswith('PASS')
    assert log.count('child_calls=1 preserved_error=planned_child_failure parent_result=43') == 8
    assert log.count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 records=25 result=43') == 8
    assert log.count('ARCHIVE_CHILD_RECLAIMED terminal_receipts=1 result_bytes=0') == 4
    assert log.count('ARCHIVE_COLLECTION stage=next original_entry_receipts_removed=10 live_records=3 logical_records=10') == 4
    assert log.count('ARCHIVE_COLLECTION stage=finish original_entry_receipts_removed=13 live_records=3 logical_records=20') == 2
    assert log.count('ARCHIVE_COLLECTION stage=finish original_entry_receipts_removed=15 live_records=3 logical_records=22') == 2
    review.update(verdict='PASS eight native failed-child continuation cases', cases={name: float(seconds) for name, seconds in passed}, log_sha256=digest(artifact / 'race.log'), binary_sha256=binary['sha256'], command_receipt=row, scope='Frozen eight-case functional race matrix only. No full simulation, broad fault/scale/soak, actual100000-entry boundary, public admission or production collection acceptance.')
else:
    mode = args.stage
    artifact = root / mode
    row = next(r for r in state['commands'] if '--root' in r['command'] and str(artifact) in r['command'])
    assert row['actual_exit_code'] == 0 and row['finished']
    stage_before = json.loads((artifact / 'source-before.json').read_text())
    assert stage_before == json.loads((artifact / 'source-after.json').read_text())
    assert stage_before['revision'] == source and stage_before['files'] == before['files']
    binary = json.loads((artifact / 'binary.json').read_text())
    race = mode == 'race1000'
    assert binary['race_instrumented'] == race and ('-race=true' in binary['build_info']) == race
    assert digest(artifact / 'sim.test') == binary['binary_sha256']
    assert all(r['exit_code'] == 0 for r in json.loads((artifact / 'command-results.json').read_text()))
    pins = sorted(n for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))
    assert sorted((artifact / 'tier1-regression-inventory.txt').read_text().splitlines()) == pins and len(pins) == 841
    output = base / (mode + '-independent-result.json')
    subprocess.run(['python3', str(checkout / 'scripts/check-tier1-suite.py'), '--events', str(artifact / 'tier1-events.jsonl'), '--inventory', str(artifact / 'tier1-inventory.txt'), '--regressions', str(artifact / 'tier1-regression-inventory.txt'), '--source', str(artifact / 'tier1-source.txt'), '--seeded-inventory', str(artifact / 'tier1-seeded-inventory.txt'), '--seeds', '1000', '--output', str(output)], check=True)
    result = json.loads(output.read_text())
    assert result == json.loads((artifact / 'tier1-result.json').read_text())
    proof = result['per_workload_seed_proof']
    assert result['source'] == source and result['pinned_regressions_pass'] == 841
    assert proof['workloads'] == 156 and proof['completed_bodies'] == 156000 and proof['first'] == 1 and proof['last'] == 1000
    ends = []
    for line in (artifact / 'tier1-events.jsonl').open():
        event = json.loads(line)
        assert event['Action'] != 'fail' and 'WARNING: DATA RACE' not in event.get('Output', '')
        if event['Action'] == 'pass' and 'Test' not in event:
            ends.append(event)
    assert len(ends) == 1
    review.update(verdict='PASS complete frozen156 ' + mode, elapsed_seconds=ends[0]['Elapsed'], binary_sha256=binary['binary_sha256'], events_sha256=digest(artifact / 'tier1-events.jsonl'), suite_result=result, command_receipt=row, scope='Complete frozen156 default suite only. Extended seeds, native/broad fault/scale/soak, actual100000-entry boundary, public admission and production collection remain separate.')
(base / (args.stage + '-review.json')).write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
