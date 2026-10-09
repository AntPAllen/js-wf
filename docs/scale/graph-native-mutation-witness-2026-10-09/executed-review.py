import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
manifest = json.loads((base / 'source-before.json').read_text())
test = 'internal/graphpublication/nats_mutation_witness_test.go'
for name, digest in manifest['files'].items():
    data = (repo / name).read_bytes()
    if name == test:
        observed = (base / 'package-observed-mutation-test.go.txt').read_bytes()
        assert data.startswith(observed), name
        data = observed
    assert hashlib.sha256(data).hexdigest() == digest, name
baseline = (base / 'baseline.log').read_text()
assert json.loads((base / 'baseline.exit.json').read_text())['exit_code'] == 1
assert len(re.findall(r'^\s*--- FAIL: TestNativeGraphMutationAcrossReadWitnesses/R[13]/(?:root|blob)/witness ', baseline, re.M)) == 4
assert len(re.findall(r'^\s*--- PASS: TestNativeGraphMutationAcrossReadWitnesses/R[13]/(?:root|blob)/replacement ', baseline, re.M)) == 4
logs = {}
for name in ['focused-race', 'package-race', 'retry-stops-race']:
    path = base / (name + '.log')
    text = path.read_text()
    assert json.loads((base / (name + '.exit.json')).read_text())['exit_code'] == 0
    assert re.search(r'^ok\s+js-wf/internal/graphpublication\s+[0-9.]+s$', text, re.M)
    assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:', text, re.M)
    logs[name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'top_level_passes': len(re.findall(r'^--- PASS:', text, re.M)), 'duration': re.findall(r'^ok\s+js-wf/internal/graphpublication\s+([0-9.]+s)$', text, re.M)}
text = (base / 'package-race.log').read_text()
for replicas in [1, 3]:
    for kind in ['root', 'blob']:
        for mode in ['witness', 'replacement']:
            name = f'TestNativeGraphMutationAcrossReadWitnesses/R{replicas}/{kind}/{mode}'
            assert re.search(r'^\s*--- PASS: ' + re.escape(name) + r' \(', text, re.M), name
        name = f'TestNativeGraphAbsentMutationAcrossReadWitnesses/R{replicas}/{kind}'
        assert re.search(r'^\s*--- PASS: ' + re.escape(name) + r' \(', text, re.M), name
assert text.count('mode=witness publication_attempts=4') == 4
assert text.count('mode=replacement publication_attempts=1') == 4
assert text.count('attempts=4 logical_revision=1') == 4
assert re.search(r'^--- PASS: TestNativeGraphAuthorityLostMutationReplies \(', text, re.M)
stop = (base / 'retry-stops-race.log').read_text()
assert 'mode=exhaustion attempts=16 logical_head=0' in stop
assert 'mode=wrapped-conflict attempts=1 logical_head=0' in stop
cli = json.loads((base / 'cli-source-before.json').read_text())
for name, digest in cli['files'].items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest, name
review = {'verdict': 'PASS native physical mutation retry controls and complete authority package race', 'compiled_package_repository_inputs_verified': len(manifest['files']), 'package_test_source': 'Exact captured source; two subsequently appended retry-stop controls qualified separately under race.', 'later_cli_repository_inputs_unchanged': len(cli['files']), 'logs': logs, 'scope': 'Native adapter only. Matching default-four-CPU combined CLI gate remains pending separately; full current-source simulation and original broader gates remain open.'}
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
