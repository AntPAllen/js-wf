"""Verify complete failed soak originals, executed source, SDK and audit traces."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = a.root.resolve()
def read(name):
    return json.loads((root / name).read_text())
def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
def seconds(start, end):
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()

execution = read('execution.json')
assert execution['source'] == '18ddf0454de13a2d5df2c1a8b22efc5f765f2de0'
assert execution['status'] == 'failed' and execution['test_exit_code'] == 1
assert execution['duration'] == '24h' and execution['row'] == 'journal'
assert execution['seed'] == 1 and execution['race'] and execution['retained_audit_trace']
manifest = read('archive-manifest.json')
archive = root / 'originals.tar.gz'
assert sha(archive) == manifest['archive_sha256']
seen = set()
member_bytes = 0
with tarfile.open(archive, 'r:gz') as tar:
    for member in tar:
        assert member.isfile() and member.name in manifest['files'] and member.name not in seen
        assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
        seen.add(member.name)
        h = hashlib.sha256()
        count = 0
        with tar.extractfile(member) as f:
            for block in iter(lambda: f.read(1024 * 1024), b''):
                h.update(block)
                count += len(block)
        assert count == member.size and h.hexdigest() == manifest['files'][member.name], member.name
        member_bytes += count
assert seen == set(manifest['files'])
# Trace assertions below apply to original bytes, not editable local substitutes.
for name in ['execution.json', 'binary.json', 'source-before.json', 'source-after.json', 'events.jsonl', 'fixture/checkpoint-audits.json'] + [f'fixture/audit-batch-100-attempt-{n}-trace.json' for n in range(1, 4)]:
    assert sha(root / name) == manifest['files'][name], name
before, after = read('source-before.json'), read('source-after.json')
assert before == after and before['revision'] == execution['source']
for name, digest in before['files'].items():
    assert manifest['files']['source/' + name] == digest
    assert hashlib.sha256(subprocess.check_output(['git', 'show', execution['source'] + ':' + name], cwd=a.repo)).hexdigest() == digest, name
binary = read('binary.json')
assert binary['race'] and '-race=true' in binary['build_info']
assert sha(root / 'integration.test') == binary['sha256'] == manifest['files']['integration.test']
actual = subprocess.check_output(['go', 'version', '-m', str(root / 'integration.test')], text=True).splitlines()
recorded = binary['build_info'].splitlines()
assert actual[0].split(': ', 1)[1] == recorded[0].split(': ', 1)[1]
assert actual[1:] == recorded[1:]
events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
failed = [e for e in events if e['Action'] == 'fail' and e.get('Test') == 'TestFiveContainerMixedJournalLeaderEveryThirtySeconds']
assert len(failed) == 1
messages = ''.join(e.get('Output', '') for e in events)
assert 'intermediate retained audit batch=100 cutoff=2800' in messages
assert messages.index('tier3 checkpoint failed:') < messages.index('batch-104')
audit = read('fixture/checkpoint-audits.json')[-1]
assert audit['batch'] == 100 and audit['invocation_cutoff'] == 2800
assert audit['error'] == 'retained audit: context deadline exceeded'
assert len(audit['attempts']) == 3
attempt_seconds = []
traces = []
for n, attempt in enumerate(audit['attempts'], 1):
    elapsed = seconds(attempt['started'], attempt['completed'])
    budget = seconds(attempt['started'], attempt['deadline'])
    assert abs(budget - 20) < .01 and 0 < elapsed <= 20.1
    attempt_seconds.append(elapsed)
    trace = read(f'fixture/audit-batch-100-attempt-{n}-trace.json')
    assert len(trace['recent']) <= 64
    for count in trace['counts'].values():
        assert count['started'] == count['completed'] and count['errors'] <= count['completed']
    traces.append(trace)
assert 8 < attempt_seconds[0] < 8.2 and abs(attempt_seconds[1] - 20) < .1 and 8 < attempt_seconds[2] < 8.2
for index in [0, 2]:
    assert audit['attempts'][index]['error'] == 'context deadline exceeded'
    assert audit['attempts'][index]['report'] == dict(Invocations=2800, Journals=0, Entries=0, Terminal=0)
    assert traces[index]['counts']['WF_JRN.GetMsg']['errors'] > 0
middle = audit['attempts'][1]
assert middle['report'] == dict(Invocations=2800, Journals=2737, Entries=29188, Terminal=2736)
assert middle['error'].endswith('terminal state missing: context deadline exceeded')
assert traces[1]['counts']['WF_JRN.GetMsg']['completed'] == 31154
assert traces[1]['counts']['WF_JRN.GetMsg']['errors'] == 0
assert traces[1]['counts']['WF_STATE.Get']['completed'] == 2751
assert traces[1]['counts']['WF_STATE.Get']['errors'] == 15
total = seconds(audit['started'], audit['completed'])
assert 38 < total < 38.2 and total < 60
summary = dict(source=execution['source'], row='journal', seed=1, requested_duration='24h', named_test_seconds=failed[0]['Elapsed'], test_exit_code=1, failed_checkpoint_batch=100, invocation_cutoff=2800, attempt_seconds=attempt_seconds, total_audit_seconds=total, attempt_budget_seconds=20, total_budget_seconds=60, three_attempt_limit=True, journal_read_counts=[t['counts']['WF_JRN.GetMsg'] for t in traces], middle_report=middle['report'], middle_error=middle['error'], source_inputs=len(before['files']), source_before_after_matches_git=True, actual_sdk_sha256=binary['sha256'], all_actual_sdk_build_info_fields_match=True, archive_members=len(seen), original_member_bytes=member_bytes, archive_bytes=archive.stat().st_size, archive_sha256=manifest['archive_sha256'], all_original_member_hashes_verified=True, physical_stores_reopened=False, cause_confirmed=False, qualifies_24h_row=False, qualifies_full_matrix=False, scope='Instrumented attempt failed. Error-free journal scan exhausts its 20-second attempt during terminal-state validation; other attempts fail journal reads. This establishes neither missing terminal state nor NATS cause. No audit budget relaxation.')
a.output.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
