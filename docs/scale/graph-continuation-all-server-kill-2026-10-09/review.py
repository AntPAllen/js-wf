"""Verify native SIGKILL cut receipts, immutable prefixes and exact redelivery."""
import hashlib
import json
import subprocess
from pathlib import Path

base = Path(__file__).resolve().parent
repo = base.parents[2]
command = json.loads((base / 'command.json').read_text())
assert command['exit'] == 0 and '-race' in command['command']
assert command['command'][command['command'].index('-run') + 1] == '^TestNativeGraphContinuationLimitAllServerSIGKILL$'
fixture = (repo / 'worker/graph_continuation_limit_kill_test.go').read_bytes()
assert hashlib.sha256(fixture).hexdigest() == command['test_sha256']
log = (base / 'race.log').read_text()
assert all(marker not in log for marker in ['--- FAIL:', 'WARNING: DATA RACE', 'panic:', 'context deadline exceeded'])
summary = {}
for cut, count, kind in [('after_signal', 14, 'SignalConsumed'), ('after_completion', 15, 'StepCompleted'), ('after_failed', 16, 'Failed')]:
    assert '--- PASS: TestNativeGraphContinuationLimitAllServerSIGKILL/' + cut in log
    directory = base / 'cuts' / cut
    read = lambda name: json.loads((directory / name).read_text())
    report, ack, event = read('recovery.json'), read('ack.json'), read('child.cut')
    prefix, records = read('prefix.json'), read('journal.json')
    assert report['sigkill'] and 0 < report['recovery_ns'] < 30_000_000_000
    assert report['lease_ttl_ns'] == 12_000_000_000
    assert len(prefix) == count and len(records) == report['entries'] == 16
    assert prefix[-1]['kind'] == kind and prefix[-1]['index'] == count - 1
    assert read('restored-prefix.json') == prefix
    assert report['servers_sigkilled'] and not report['servers_gracefully_restarted']
    deaths = read('server-kills.json')
    assert len(deaths) == 3 and all(row['Signal'] == 9 and row['OldPID'] > 0 and row['NewPID'] > 0 and row['OldPID'] != row['NewPID'] for row in deaths)
    assert len({row['OldPID'] for row in deaths}) == len({row['NewPID'] for row in deaths}) == 3
    assert records[:count] == prefix and records[-1]['kind'] == 'Failed'
    assert records[-1]['payload']['error'] == 'journal exceeds 100000 entries'
    assert records[-1]['payload']['limit_request']['kind'] == 'run'
    assert records[-1]['payload']['limit_request']['name'] == 'must_not_run'
    for index, record in enumerate(records):
        assert record['index'] == index
        if index:
            assert record['sequence'] == records[index - 1]['sequence'] + 1
            assert record['epoch'] >= records[index - 1]['epoch']
    assert event['Operation'] == 'journal_append' and event['Error'] == ''
    assert event['JournalIndex'] == count - 1 and event['JournalKind'] == kind
    assert event['RunSequence'] == report['cut_run_sequence'] == report['ack_run_sequence'] == ack['RunSequence']
    assert ack['Delivery'] == report['ack_delivery'] > report['cut_delivery'] == event['Delivery']
    assert ack['Stage'] == 'ack' and ack['Error'] == '' and ack['Worker'] == 'graph-limit-successor'
    held = read('held-lease.json')
    assert held['worker'] == 'graph-limit-killed'
    assert held['epoch'] == report['held_epoch']
    assert records[-1]['epoch'] == report['terminal_epoch']
    if cut != 'after_failed':
        assert report['terminal_epoch'] > report['held_epoch']
        assert records[-1]['worker_id'] == 'graph-limit-successor'
    handlers = (directory / 'child.handlers').read_text().splitlines()
    assert 'effect' not in handlers and handlers.count('initial') == handlers.count('middle') == 1
    summary[cut] = dict(recovery_seconds=report['recovery_ns'] / 1e9,
                        exact_ack_sequence=ack['RunSequence'], exact_ack_delivery=ack['Delivery'],
                        terminal_epoch=report['terminal_epoch'], held_epoch=report['held_epoch'])
binaries = json.loads((base / 'server-binaries.json').read_text())
assert len({row['sha256'] for row in binaries.values()}) == 1
for row in binaries.values():
    assert hashlib.sha256(Path(row['path']).read_bytes()).hexdigest() == row['sha256']
result = dict(production_revision=command['head'], test_sha256=command['test_sha256'],
              native_R3_domain_all_server_SIGKILL_cases=summary,
              scope='Three worker SIGKILL cuts plus verified SIGKILL of all three server processes, original-store reopen, native R3 domain/archive, private16 budget, exact redelivery and strict <30s. Host kernel/page cache stayed live; no power/storage/VM fault or actual100000 boundary claim.')
(base / 'executed-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
