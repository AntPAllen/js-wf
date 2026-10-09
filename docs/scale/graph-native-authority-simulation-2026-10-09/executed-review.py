import hashlib
import itertools
import json
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
repo = base.parents[2]
manifest = json.loads((base / 'source-before.json').read_text())
revision = subprocess.check_output(['git', 'rev-parse', '13a0fd6'], cwd=repo, text=True).strip()
process = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
def git_bytes(revision, name):
    process.stdin.write((revision + ':' + name + '\n').encode())
    process.stdin.flush()
    header = process.stdout.readline().split()
    assert len(header) == 3 and header[1] == b'blob', name
    result = process.stdout.read(int(header[2]))
    assert process.stdout.read(1) == b'\n'
    return result
try:
    for name, digest in manifest['files'].items():
        assert hashlib.sha256(git_bytes(revision, name)).hexdigest() == digest, name
    old = (base / 'baseline-nats_authority.go.txt').read_bytes()
    assert old == git_bytes('6df82898e70c5cd2a71bea384910f4505df365f6', 'internal/graphpublication/nats_authority.go')
    pins = {name: digest for name, digest in manifest['files'].items() if name.startswith('sim/testdata/regressions/') and name.endswith('.json')}
    prior = {name: digest for name, digest in pins.items() if not name.startswith('sim/testdata/regressions/native-authority-')}
    assert len(prior) == 786 and len(pins) == 810
    for name, digest in prior.items():
        assert hashlib.sha256(git_bytes('4f1f2b2', name)).hexdigest() == digest, name
finally:
    process.stdin.close()
    assert process.wait() == 0
assert len((base / 'seeded-inventory.txt').read_text().splitlines()) == 151
assert 'TestSeededGraphNativeAuthorityReplay' in (base / 'seeded-inventory.txt').read_text().splitlines()
expected = {'/'.join(case) for case in itertools.product(['root', 'blob'], ['absent', 'present'], ['healthy', 'witness', 'replacement', 'exhaustion', 'drop', 'lost'])}
logs = {}
for name, exit_name in [('development/generated-pins-normal1000', 'development/generated-pins-normal1000'), ('race1000', 'race1000'), ('pins-normal', 'pins-normal'), ('pins-race', 'pins-race'), ('baseline-minimize', 'baseline-minimize')]:
    assert json.loads((base / (exit_name + '.exit.json')).read_text())['exit_code'] == 0
    path = base / (name + '.log')
    text = path.read_text()
    assert re.search(r'^ok\s+js-wf/sim\s+[0-9.]+s$', text, re.M)
    assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)|WARNING: DATA RACE|^panic:', text, re.M)
    if name.endswith('1000'):
        assert 'TIER1_SEEDS test=TestSeededGraphNativeAuthorityReplay first=1 last=1000 completed=1000 requested=1000' in text
        counts = dict((case, int(count)) for case, count in re.findall(r'((?:root|blob)/(?:absent|present)/(?:healthy|witness|replacement|exhaustion|drop|lost)):(\d+)', text))
        assert set(counts) == expected and sum(counts.values()) == 1000 and all(count > 0 for count in counts.values())
    if name.startswith('pins-'):
        passed = re.findall(r'^\s*--- PASS: TestPinnedRegressionCorpus/(\S+) \(', text, re.M)
        assert len(passed) == len(set(passed)) == 24
        assert set(passed) == {Path(name).name for name in pins if name.startswith('sim/testdata/regressions/native-authority-')}
    logs[name] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'duration': re.findall(r'^ok\s+js-wf/sim\s+([0-9.]+s)$', text, re.M)}
assert json.loads((base / 'baseline.exit.json').read_text())['exit_code'] == 1
baseline = (base / 'baseline.log').read_text()
assert 'FAULT_SEED=1' in baseline and 'mode=root/absent/witness attempts=1 want=2' in baseline
failed = list(base.glob('baseline-failure.*.json'))
assert len(failed) == 1
trace = json.loads(failed[0].read_text())
assert trace['workload'] == 'native_authority_witness' and trace['decisions'][0]['chosen'] == 'root/absent/witness'
assert any(event['operation'] == 'native_authority_mutation' and event['outcome'] == 'conflict' for event in trace['transport'])
assert 'in 4 reproductions' in (base / 'baseline-minimize.log').read_text()
review = {'verdict': 'PASS native adapter seeded family normal/race and all 24 new regression traces normal/race', 'source_revision': revision, 'repository_inputs_git_verified': len(manifest['files']), 'canonical_seeded_families': 151, 'saved_regressions': 810, 'prior_pins_git_verified_unchanged': len(prior), 'mode_combinations': len(expected), 'baseline': {'source_file_revision': '6df82898e70c5cd2a71bea384910f4505df365f6', 'seed': 1, 'go_test_seconds': 0.007, 'minimizer_reproductions': 4, 'scope': 'One-file old-production overlay against current model; invalid generation fixture and invalid pin output path are development harness failures, not runtime evidence.'}, 'logs': logs, 'scope': 'Single durable SDK-boundary store model, not NATS replication/routing/fsync conformance. No all-810-pin/current-full/extended/native fault/scale/soak/drain/migration/adoption/release acceptance is inferred. The earlier race1000 command exercised the seeded family only; new pin checks use separate exact subtest filters.'}
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
