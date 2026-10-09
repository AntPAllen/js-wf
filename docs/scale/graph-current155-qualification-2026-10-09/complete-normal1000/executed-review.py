import hashlib
import json
import re
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[3]
source = '1f316a2b0397db6ff84bebe984a4b1428e9c0770'
before = json.loads((base / 'source-before.json').read_text())
assert before == json.loads((base / 'source-after.json').read_text()) and before['revision'] == source
process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    for name, digest in before['files'].items():
        process.stdin.write((source + ':' + name + '\n').encode())
        process.stdin.flush()
        header = process.stdout.readline().split()
        assert len(header) == 3 and header[1] == b'blob', name
        content = process.stdout.read(int(header[2]))
        assert process.stdout.read(1) == b'\n'
        assert hashlib.sha256(content).hexdigest() == digest, name
finally:
    process.stdin.close()
    assert process.wait() == 0
binary = json.loads((base / 'binary.json').read_text())
assert not binary['race_instrumented'] and '-race=true' not in binary['build_info']
retained = Path('/home/exedev/js-wf-tier1-full155-normal1000-20261009/sim.test')
assert hashlib.sha256(retained.read_bytes()).hexdigest() == binary['binary_sha256']
assert json.loads((base / 'supervisor-launch-exit.json').read_text())['actual_exit_code'] == 0
command = ['python3', 'scripts/check-tier1-suite.py', '--events', str(base / 'tier1-events.jsonl'), '--inventory', str(base / 'tier1-inventory.txt'), '--regressions', str(base / 'tier1-regression-inventory.txt'), '--source', str(base / 'tier1-source.txt'), '--seeded-inventory', str(base / 'tier1-seeded-inventory.txt'), '--seeds', '1000', '--output', str(base / 'independent-suite-result.json')]
subprocess.run(command, cwd=repo, check=True, stdout=subprocess.DEVNULL)
result = json.loads((base / 'independent-suite-result.json').read_text())
assert result == json.loads((base / 'tier1-result.json').read_text())
assert result['source'] == source and result['top_level_pass'] == 222 and result['pinned_regressions_pass'] == 835
proof = result['per_workload_seed_proof']
assert proof['workloads'] == 155 and proof['completed_bodies'] == 155000 and proof['first'] == 1 and proof['last'] == 1000
assert 'TestSeededGraphNativeAuthorityReplay' in proof['tests'] and 'TestSeededGraphAwaitContentionReplay' in proof['tests']
assert set(result['trace_only_skips']) == {'TestReplayFaultTrace', 'TestMinimizeFaultTrace'}
assert result['events_sha256'] == hashlib.sha256((base / 'tier1-events.jsonl').read_bytes()).hexdigest()
ends = []
cartesian = []
for line in (base / 'tier1-events.jsonl').open():
    event = json.loads(line)
    assert event['Action'] != 'fail' and 'WARNING: DATA RACE' not in event.get('Output', '')
    if event['Action'] == 'pass' and 'Test' not in event:
        ends.append(event)
    if 'SIGNAL_CARTESIAN_COMPLETE completed=336 requested=336 beyond_first_1000=16' in event.get('Output', ''):
        cartesian.append(event)
assert len(ends) == 1 and len(cartesian) == 1
assert all(row['exit_code'] == 0 for row in json.loads((base / 'command-results.json').read_text()))
contexts = json.loads((base / 'execution-contexts.json').read_text())
execution = next(row for row in contexts if 'test2json' in row['command'])
assert execution['working_directory'] == '/home/exedev/js-wf-current155-qualification/sim'
tuple_counts = {}
for test, count in [('TestSeededGraphSignalOperationActorsReplay', 5), ('TestSeededGraphSignalExpiryActorsReplay', 30), ('TestSeededGraphReaderMaintenanceReplay', 48), ('TestSeededGraphTerminalAuditReplay', 11)]:
    rows = [json.loads(line) for line in (base / 'tier1-events.jsonl').open()]
    text = ''.join(row.get('Output', '') for row in rows if row.get('Test') == test)
    maps = re.findall(r'map\[([^\]]+)\]', text)
    assert len(maps) == 1, test
    counts = {name: int(value) for name, value in re.findall(r'([^ ]+):(\d+)', maps[0])}
    assert len(counts) == count and all(value > 0 for value in counts.values()) and sum(counts.values()) == 1000, (test, counts)
    tuple_counts[test] = counts
review = {'verdict': 'PASS complete frozen155 normal default simulation', 'source': source, 'repository_inputs_git_verified_unchanged': len(before['files']), 'top_groups_pass': 222, 'seeded_families': 155, 'completed_seed_bodies': 155000, 'saved_regressions_pass': 835, 'directed_signal_combinations': 336, 'retained_binary_sha256_verified': binary['binary_sha256'], 'elapsed_seconds': ends[0]['Elapsed'], 'events_sha256_verified': result['events_sha256'], 'added_family_mode_counts': tuple_counts, 'scope': 'Full normal frozen source 1f316a2. Later fallback namespace family, scoped maintenance/repair, envelope guard, traversal/reader refresh and native continuation limit/kill fixtures excluded. Full current 156/all841, race/extended and every original native/runtime/fault/scale/soak/drain/import/adoption/release requirement remain separate.'}
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
