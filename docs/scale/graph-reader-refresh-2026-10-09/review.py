"""Check retained selected-native results and their frozen repository inputs."""
import hashlib
import json
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
r3 = json.loads((base / 'native-command-results.json').read_text())
r1 = json.loads((base / 'native-r1-command.json').read_text())
before = json.loads((base / 'native-source-before.json').read_text())
after = json.loads((base / 'native-source-after.json').read_text())
assert before == after and before['revision'] == r3['revision'] == r1['revision']
assert r3['actual_test_exit'] == r1['exit'] == 0
assert len(r3['commands']) == 2 and all(c['exit'] == 0 for c in r3['commands'])
assert r3['commands'][0]['command'][1:5] == ['test', '-race', '-c', './worker']
assert r3['commands'][1]['command'][1:] == ['-test.run=^TestNativeGraphCheckpointMaterializedReferences/R3Domain$', '-test.count=1', '-test.timeout=3m', '-test.v']
assert r1['command'][1:] == ['-test.run=^TestNativeGraphCheckpointMaterializedReferences/R1$', '-test.count=1', '-test.timeout=3m', '-test.v']
assert r3['source_unchanged'] and '-race=true' in (base / 'native-binary-build.txt').read_text()
binary = Path(r3['commands'][1]['command'][0])
assert r1['command'][0] == str(binary)
assert hashlib.sha256(binary.read_bytes()).hexdigest() == r3['binary_sha256'] == r1['binary_sha256']
checkout = Path(r3['checkout'])
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == r3['revision']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
names = list(before['files'])
batch = subprocess.run(['git', 'cat-file', '--batch'], cwd=checkout,
                       input=''.join(r3['revision'] + ':' + name + '\n' for name in names).encode(),
                       stdout=subprocess.PIPE, check=True).stdout
offset = 0
for name in names:
    header_end = batch.index(b'\n', offset)
    header = batch[offset:header_end].split()
    assert header[1] == b'blob'
    size = int(header[2])
    data = batch[header_end + 1:header_end + 1 + size]
    assert hashlib.sha256(data).hexdigest() == before['files'][name], name
    assert hashlib.sha256((checkout / name).read_bytes()).hexdigest() == before['files'][name], name
    offset = header_end + 1 + size + 1
assert offset == len(batch)
for name, subtest in [('native-r3-race.log', 'R3Domain'), ('native-r1-race.log', 'R1')]:
    text = (base / name).read_text()
    assert '--- PASS: TestNativeGraphCheckpointMaterializedReferences/' + subtest in text
    assert 'promise aliases=2 completion_payload_edges=3 materialized_index=132 blocked_prefix_reads=0' in text
    assert 'CHECKPOINT_PHASE phase=bounded_delivery_restore' in text
    assert 'CHECKPOINT_PHASE phase=worker_execute_1' in text
    assert 'CHECKPOINT_PHASE phase=await_and_final_assertions' in text
    assert text.rstrip().endswith('PASS')
    assert all(marker not in text for marker in ['--- FAIL:', 'WARNING: DATA RACE', 'panic:'])
result = dict(revision=r3['revision'], repository_inputs_match_git=len(names),
              binary_sha256=r3['binary_sha256'], native_R1_R3_materialized_fixture_pass=True,
              scope='Selected native fixture on frozen 9cbdfbc; not full current-source qualification or public admission.')
(base / 'executed-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
