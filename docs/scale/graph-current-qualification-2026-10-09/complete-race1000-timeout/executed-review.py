import hashlib
import json
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[3]
before = json.loads((base / 'source-before.json').read_text())
assert before == json.loads((base / 'source-after.json').read_text())
assert before['revision'] == 'fca8264d2229144e9b3e6a70df747d1307666fe6'
process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    for name, digest in before['files'].items():
        process.stdin.write((before['revision'] + ':' + name + '\n').encode())
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
assert binary['race_instrumented'] and '-race=true' in binary['build_info']
assert hashlib.sha256(Path('/home/exedev/js-wf-tier1-full149-race1000-20261009/sim.test').read_bytes()).hexdigest() == binary['binary_sha256']
assert json.loads((base / 'exit.json').read_text()) == {'supervisor_exit_code': 1, 'test2json_exit_code': 1}
review = json.loads((base / 'review.json').read_text())
assert review['source'] == before['revision']
assert review['repository_inputs_git_verified_unchanged'] == len(before['files'])
assert review['event_sha256'] == hashlib.sha256((base / 'tier1-events.jsonl').read_bytes()).hexdigest()
completed = []
failed = []
panic = []
top_passes = 0
for line in (base / 'tier1-events.jsonl').open():
    event = json.loads(line)
    output = event.get('Output', '')
    assert 'WARNING: DATA RACE' not in output and '--- FAIL:' not in output
    if 'TIER1_SEEDS ' in output:
        fields = dict(token.split('=', 1) for token in output.split('TIER1_SEEDS ', 1)[1].split())
        assert fields['first'] == '1' and fields['last'] == fields['completed'] == fields['requested'] == '1000'
        completed.append(fields['test'])
    if event['Action'] == 'fail':
        failed.append(event)
    if 'panic: test timed out after 3h0m0s' in output:
        panic.append(output.strip())
    if event['Action'] == 'pass' and event.get('Test') and '/' not in event['Test']:
        top_passes += 1
assert len(completed) == len(set(completed)) == review['completed_seed_families'] == 89
assert len((base / 'tier1-seeded-inventory.txt').read_text().splitlines()) == review['required_seed_families'] == 149
assert set(completed) <= set((base / 'tier1-seeded-inventory.txt').read_text().splitlines())
assert failed == review['failed_actions'] and panic == review['panic'] and top_passes == review['completed_top_groups']
print(json.dumps(review))
