"""Review the closed copied-store experiment, without accepting full scale."""
import gzip
import hashlib
import json
from pathlib import Path

out = Path(__file__).resolve().parent
root = Path('/home/exedev/js-wf-canonical-start-retained-recovery-20261011')
receipt = json.loads((out / 'run-command.json').read_text())
assert not Path('/proc/' + str(receipt['pid'])).exists()
assert receipt['source_unchanged']
assert hashlib.sha256((root / 'recover').read_bytes()).hexdigest() == receipt['binary_sha256']
assert hashlib.sha256((out / 'recover.go.txt').read_bytes()).hexdigest() == receipt['helper_sha256']
inputs = json.loads((out / 'source-inputs.json').read_text())
for name, sha in inputs.items():
    assert hashlib.sha256((root / 'source' / name).read_bytes()).hexdigest() == sha, name

def census(directory):
    return {str(p.relative_to(directory)): {
        'bytes': p.stat().st_size,
        'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
    } for p in directory.rglob('*') if p.is_file()}

marker = json.loads((out / 'copy-verified.json').read_text())
original = census(Path(marker['original']))
expected = json.loads((Path(marker['original']).parent / 'native-census.json').read_text())
assert original == expected
assert len(original) == marker['files'] == 2443
assert sum(v['bytes'] for v in original.values()) == marker['bytes'] == 198307813
copied = census(root / 'store-copy')
(out / 'closed-copy-census.json.gz').write_bytes(gzip.compress(
    (json.dumps(copied, sort_keys=True, indent=2) + '\n').encode(), mtime=0))
result = dict(actual_exit=receipt['actual_exit'], original_unchanged=True,
              original_files=len(original), original_bytes=marker['bytes'],
              source_inputs=len(inputs), binary_sha256=receipt['binary_sha256'],
              copy_files=len(copied), copy_bytes=sum(v['bytes'] for v in copied.values()),
              full_scale_accepted=False, failure_cause_confirmed=False)
if receipt['actual_exit'] == 0:
    proof = json.loads((out / 'result.json').read_text())
    assert proof['PendingBefore'] > 0
    assert proof['PendingBefore'] == proof['Recovered'] == len(proof['Rows'])
    seen = set()
    for row in proof['Rows']:
        before, after = row['Before'], row['After']
        a, b = before['State'], after['State']
        descriptor = a['Start']
        identity = (descriptor['request']['type'], descriptor['request']['id'])
        assert identity not in seen
        seen.add(identity)
        assert a['Pending'] and a['Invocation'] == 0
        assert not b['Pending'] and b['Start'] == descriptor
        assert b['Invocation'] == row['SourceAfter'] == row['Handle']['InvSeq'] > 0
        assert not row['SourceBefore'] or row['SourceBefore'] == row['SourceAfter']
        for key in ('JournalCount', 'JournalTail', 'Kind', 'Retired', 'Purging'):
            assert before[key] == after[key]
        data = json.dumps(row['Input'], separators=(',', ':'), ensure_ascii=False).encode()
        assert len(data) == descriptor['input_size']
        assert hashlib.sha256(data).hexdigest() == descriptor['input_sha256']
    assert proof['InvocationsAfter'] - proof['InvocationsBefore'] == proof['Recovered'] - proof['SourceAlreadyPresent']
    assert proof['RunLastAfter'] - proof['RunLastBefore'] == proof['Recovered']
    result.update(pending=proof['PendingBefore'], recovered=proof['Recovered'],
                  ready_preserved=proof['ReadyBefore'], recovery_verified=True)
else:
    assert (out / 'run.log').read_text().strip()
    result.update(recovery_verified=False, failure_log='run.log')
(out / 'review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
