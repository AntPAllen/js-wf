import hashlib
import itertools
import json
import re
import subprocess
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
manifest = json.loads((base / 'source-before.json').read_text())
frozen_path = base / 'frozen-source-review.json'
if frozen_path.exists():
    frozen = json.loads(frozen_path.read_text())
    assert frozen['manifest_sha256'] == hashlib.sha256((base / 'source-before.json').read_bytes()).hexdigest()
    process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name, digest in manifest['files'].items():
            process.stdin.write((frozen['source_revision'] + ':' + name + '\n').encode())
            process.stdin.flush()
            header = process.stdout.readline().split()
            assert len(header) == 3 and header[1] == b'blob', name
            content = process.stdout.read(int(header[2]))
            assert process.stdout.read(1) == b'\n'
            assert hashlib.sha256(content).hexdigest() == digest, name
    finally:
        process.stdin.close()
        assert process.wait() == 0
    assert frozen['repository_inputs_git_verified'] == len(manifest['files'])
    binary = json.loads((base / 'race-binary.json').read_text())
    assert binary['source_revision'] == frozen['source_revision']
    assert hashlib.sha256(Path(binary['retained_binary']).read_bytes()).hexdigest() == binary['binary_sha256']
else:
    for name, digest in manifest['files'].items():
        assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest, name
dimensions = [
    ['healthy', 'source_drop', 'source_lost_ack', 'queue_drop', 'queue_lost_readback', 'reserved', 'batch_restart'],
    ['healthy', 'catalog_drop', 'lifecycle_unknown', 'dry_catalog'],
    ['healthy', 'enqueue_drop', 'enqueue_lost_ack'],
    ['healthy', 'consumption_lost_ack', 'consumption_unknown', 'prepared_append_repair'],
]
expected = {'/'.join(choices) for choices in itertools.product(*dimensions)}
assert len(expected) == 336
logs = {}
reference = None
for mode in ['normal', 'race']:
    exit_path = base / (mode + '.exit.json')
    if not exit_path.exists():
        continue
    assert json.loads(exit_path.read_text())['exit_code'] == 0
    path = base / (mode + '.log')
    text = path.read_text()
    assert re.search(r'^ok\s+js-wf/sim\s+[0-9.]+s$', text, re.M)
    assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:', text, re.M)
    rows = re.findall(r'SIGNAL_CARTESIAN seed=(\d+) combination=(\S+) exact_replay=true', text)
    cases = {name: int(seed) for seed, name in rows}
    assert len(rows) == len(cases) == 336 and set(cases) == expected
    passed = re.findall(r'^\s*--- PASS: TestGraphSignalRuntimeCartesianReplay/(\S+) \(', text, re.M)
    assert len(passed) == len(set(passed)) == 336 and set(passed) == expected
    beyond = sum(seed > 1000 for seed in cases.values())
    assert beyond == 16
    assert f'SIGNAL_CARTESIAN_COMPLETE completed=336 requested=336 beyond_first_1000={beyond}' in text
    if reference is not None:
        assert cases == reference
    reference = cases
    logs[mode] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'completed_combinations': len(cases), 'new_combinations_beyond_first_1000': beyond, 'maximum_selected_seed': max(cases.values()), 'duration': re.findall(r'^ok\s+js-wf/sim\s+([0-9.]+s)$', text, re.M)}
assert 'normal' in logs
review = {'verdict': 'PASS all 336 declared Signal combinations with exact replay in ' + '/'.join(logs), 'repository_inputs_verified': len(manifest['files']), 'source_revision': frozen['source_revision'] if frozen_path.exists() else manifest['revision'], 'saved_regressions_unchanged': sum(name.startswith('sim/testdata/regressions/') and name.endswith('.json') for name in manifest['files']), 'logs': logs, 'scope': 'Directed Cartesian domain supplements the unchanged 150-family contiguous seed gate. No arbitrary operation permutations, full current-source suite, extended seeds or native fault/scale/soak/drain/adoption/release qualification is inferred.'}
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
