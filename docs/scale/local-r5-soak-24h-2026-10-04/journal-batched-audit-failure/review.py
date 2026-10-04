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
assert execution['source'] == 'b287e983872e310d92bfb9dab263f0a01875a43b'
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
for name in ['execution.json', 'binary.json', 'source-before.json', 'source-after.json', 'events.jsonl', 'fixture/checkpoint-audits.json'] + [f'fixture/audit-batch-400-attempt-{n}-trace.json' for n in range(1, 4)]:
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
assert 'intermediate retained audit batch=400 cutoff=11200' in messages
assert execution['batched_retained_audit']
audit = read('fixture/checkpoint-audits.json')[-1]
assert audit['batch'] == 400 and audit['invocation_cutoff'] == 11200
assert audit['error'].endswith('terminal state missing: context deadline exceeded')
assert len(audit['attempts']) == 3
attempt_seconds = []
traces = []
for n, attempt in enumerate(audit['attempts'], 1):
    elapsed = seconds(attempt['started'], attempt['completed'])
    budget = seconds(attempt['started'], attempt['deadline'])
    assert 19.9 < budget <= 20.01 and 19.9 < elapsed <= 20.1
    attempt_seconds.append(elapsed)
    trace = read(f'fixture/audit-batch-400-attempt-{n}-trace.json')
    assert len(trace['recent']) <= 64
    for count in trace['counts'].values():
        assert count['started'] == count['completed'] and count['errors'] <= count['completed']
    assert trace['counts']['WF_INV.Fetch']['completed'] == 22 and trace['counts']['WF_INV.Fetch']['errors'] == 0
    traces.append(trace)
for index, fetched, kv, report in [
    (0, 242, 6128, dict(Invocations=11200, Journals=6128, Entries=58291, Terminal=6128)),
    (2, 244, 10128, dict(Invocations=11200, Journals=10114, Entries=95190, Terminal=10113))]:
    counts = traces[index]['counts']
    assert counts['WF_JRN.Fetch']['completed'] == fetched and counts['WF_JRN.Fetch']['errors'] == 0
    assert counts['WF_STATE.Get']['completed'] == kv
    assert 'WF_JRN.GetMsg' not in counts
    assert audit['attempts'][index]['report'] == report
assert traces[0]['counts']['WF_STATE.Get']['errors'] == 0
assert traces[2]['counts']['WF_STATE.Get']['errors'] == 15
assert audit['attempts'][0]['error'] == 'context deadline exceeded'
assert audit['attempts'][2]['error'].endswith('terminal state missing: context deadline exceeded')
middle = audit['attempts'][1]
assert middle['report'] == dict(Invocations=11200, Journals=0, Entries=0, Terminal=0)
assert middle['error'] == 'context deadline exceeded'
assert traces[1]['counts']['WF_JRN.Fetch']['completed'] == 239 and traces[1]['counts']['WF_JRN.Fetch']['errors'] == 1
assert traces[1]['counts']['WF_JRN.GetMsg']['completed'] == traces[1]['counts']['WF_JRN.GetMsg']['errors'] == 2
assert 'WF_STATE.Get' not in traces[1]['counts']
total = seconds(audit['started'], audit['completed'])
assert 60 <= total < 60.1
assert any(c['batch']==390 and not c.get('error') and c['report']==dict(Invocations=10920,Journals=10920,Entries=120353,Terminal=10920) for c in read('fixture/checkpoint-audits.json'))
summary = dict(source=execution['source'], row='journal', seed=1, requested_duration='24h', batched_retained_audit=True, named_test_seconds=failed[0]['Elapsed'], test_exit_code=1, failed_checkpoint_batch=400, invocation_cutoff=11200, attempt_seconds=attempt_seconds, total_audit_seconds=total, attempt_budget_seconds=20, total_budget_seconds=60, three_attempt_limit=True, attempts=audit['attempts'], trace_counts=[t['counts'] for t in traces], last_successful_cohort=10920, source_inputs=len(before['files']), source_before_after_matches_git=True, actual_sdk_sha256=binary['sha256'], all_actual_sdk_build_info_fields_match=True, archive_members=len(seen), original_member_bytes=member_bytes, archive_bytes=archive.stat().st_size, archive_sha256=manifest['archive_sha256'], all_original_member_hashes_verified=True, physical_stores_reopened=False, nats_cause_confirmed=False, qualifies_24h_row=False, qualifies_full_matrix=False, scope='Batched attempt failed at original20s/60s limits. Two error-free bulk scans exhaust the attempt during invariant/terminal validation; middle bulk fetch and leader-read fallback time out. No missing-state/corruption or NATS-cause claim. Fetch elapsed includes caller processing; method totals may overlap.')
a.output.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k:v for k,v in summary.items() if k not in ['attempts','trace_counts']}, indent=2))
