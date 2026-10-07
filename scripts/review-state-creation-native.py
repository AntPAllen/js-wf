#!/usr/bin/env python3
"""Review real WatchAll creation recovery bytes without starting a broker."""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

import fixture_archive
from worker_leaf_wire import protocol

REPO = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, REPO/'scripts'/filename)
    obj = importlib.util.module_from_spec(spec); spec.loader.exec_module(obj)
    return obj


clock = load('creation_clock', 'check-tier2-journal-shard.py')
closed = load('creation_closed', 'verify-tier2-closed-originals.py')


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def integer(value, expected):
    assert type(value) is int and value == expected


def validate_fixture(root):
    root = Path(root)
    proof = read(root/'native-proof.json')
    assert proof['passed'] is True
    integer(proof['attempts'], 2); integer(proof['expected_keys'], 1024)
    integer(proof['parent_budget_ns'], 20_000_000_000)
    assert type(proof['first_creation_elapsed_ns']) is int and 2_000_000_000 <= proof['first_creation_elapsed_ns'] < 3_000_000_000
    assert type(proof['elapsed_ns']) is int and proof['first_creation_elapsed_ns'] <= proof['elapsed_ns'] < proof['parent_budget_ns']
    started, returned = [clock.timestamp_ns(proof[k]) for k in ('first_creation_started', 'first_creation_returned')]
    assert 2_000_000_000 <= returned-started < 3_000_000_000 and abs(returned-started-proof['first_creation_elapsed_ns']) < 1_000_000
    assert proof['first_native_error'] == 'context canceled'
    ids = proof['server_ids']; assert len(ids) == len(set(ids)) == 3 and all(re.fullmatch('N[A-Z2-7]{55}', value) for value in ids)
    for name, disposition in [('pending_before_close', 'held'), ('pending_after_close', 'cancelled')]:
        pending = proof[name]
        integer(pending['connection'], 1); integer(pending['forwarded_bytes'], 0)
        assert pending['disposition'] == disposition
    before, after = proof['pending_before_close'], proof['pending_after_close']
    assert {k:v for k,v in before.items() if k != 'disposition'} == {k:v for k,v in after.items() if k != 'disposition'}
    pending = protocol(base64.b64decode(before['packet'], validate=True))
    assert len(pending) == 1 and pending[0][0] in (b'PUB', b'HPUB')
    _, fields, data = pending[0]
    assert fields[1].decode() == before['subject'] and before['subject'].startswith('$JS.API.CONSUMER.CREATE.KV_WF_STATE.')
    request = json.loads(data)
    assert request['stream_name'] == 'KV_WF_STATE'
    config = request['config']
    assert config['deliver_policy'] == 'last_per_subject' and config['ack_policy'] == 'none' and config['filter_subject'] == '$KV.WF_STATE.>'
    integer(config['num_replicas'], 1); assert config['mem_storage'] is True
    values = read(root/'expected-values.json')
    assert sha(root/'expected-values.json') == proof['expected_values_sha256']
    assert values == {f'included-{i:04d}':base64.b64encode(f'retained-state-{i:04d}'.encode()).decode() for i in range(1024)}
    trace, stats = proof['proxy_trace'], proof['proxy_stats']
    assert trace['truncated'] is False and not trace.get('frame_file_error') and trace['frame_file'] == 'wire.jsonl'
    assert len(trace['connections']) == 1 and trace['frames'] is None
    connection = trace['connections'][0]; integer(connection['id'], 1)
    for key in ('upstream_dial_failures', 'held_bytes', 'active_connections', 'buffered_bytes', 'buffer_overflows'):
        integer(stats[key], 0)
    integer(stats['accepted_connections'], 1); assert stats['responses_held'] is False
    streams = {k:bytearray() for k in ('client_to_server', 'server_to_client')}
    count = 0
    for line in (root/'wire.jsonl').read_text().splitlines():
        frame = json.loads(line); integer(frame['connection'], 1)
        assert frame['direction'] in streams
        clock.timestamp_ns(frame['at'])
        streams[frame['direction']].extend(base64.b64decode(frame['data'], validate=True)); count += 1
    integer(trace['frame_records'], count); assert count > 0
    for key, data in streams.items():
        integer(stats[key], len(data)); assert len(data) > 0
    outgoing, incoming = protocol(streams['client_to_server']), protocol(streams['server_to_client'], True)
    assert sum(op == b'CONNECT' for op,_,_ in outgoing) == 1 and incoming[0][0] == b'INFO'
    assert all(info['server_id'] == ids[0] for op,_,info in incoming if op == b'INFO')
    publications = [parts[1].decode() for op,parts,_ in outgoing if op in (b'PUB', b'HPUB')]
    assert '$JS.API.STREAM.INFO.KV_WF_STATE' in publications
    assert not any(subject.startswith('$JS.API.CONSUMER.CREATE.KV_WF_STATE.') for subject in publications)
    frames = proof['snapshot_observations']
    expected_events = ['watch_start', 'watch_creation_error', 'attempt_return', 'watch_start', 'watch_created', 'initial_complete', 'watch_stop_start', 'watch_stopped', 'attempt_return']
    assert [f['event'] for f in frames] == expected_events
    deadline = clock.timestamp_ns(proof['deadline'])
    assert started >= clock.timestamp_ns(frames[0]['time']) and returned <= clock.timestamp_ns(frames[1]['time'])
    assert all(f['deadline'] == proof['deadline'] for f in frames)
    times = [clock.timestamp_ns(f['time']) for f in frames]; assert times == sorted(times) and times[-1] < deadline
    for frame in frames[:3]:
        integer(frame['received'], 0); integer(frame['included'], 0)
        assert frame['initial_complete'] is False
    assert 'creation made no progress: nats: timeout' in frames[1]['error']
    assert 'initial_complete=false' in frames[2]['error']
    for frame in frames[5:]:
        integer(frame['received'], 1027); integer(frame['included'], 1026); integer(frame['last_revision'], 1029)
        assert frame['initial_complete'] is True and not frame.get('error')
    info = proof['state_stream_info']
    assert info['config']['name'] == 'KV_WF_STATE' and info['config']['storage'] == 'file'
    integer(info['config']['num_replicas'], 3); integer(info['state']['consumer_count'], 0)
    integer(info['state']['messages'], 1027); integer(info['state']['last_seq'], 1029)
    cluster = info['cluster']; assert cluster['leader'] in {f'wf-test-{i}' for i in range(3)}
    assert len(cluster['replicas']) == 2 and {r['name'] for r in cluster['replicas']} == {f'wf-test-{i}' for i in range(3)}-{cluster['leader']}
    assert all(r['current'] is True for r in cluster['replicas'])
    return dict(first_creation_elapsed_ns=proof['first_creation_elapsed_ns'], recovery_stage_elapsed_ns=proof['elapsed_ns'], wire_frames=count,
                client_bytes=len(streams['client_to_server']), server_bytes=len(streams['server_to_client']), server_ids=ids,
                exact_retained_values=1024, attempts=2, consumer_count=0, complete_initial_barrier=True,
                held_request_not_forwarded=True, scope='Controlled R3 pre-publication creation stall; healthy/control payload assertions remain native source-bound, no independent store decoder or R5/24h qualification.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--metadata', type=Path, required=True)
    parser.add_argument('--service', required=True)
    parser.add_argument('--invocation', required=True)
    parser.add_argument('--producer-pid', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert args.root.is_absolute() and not args.output.exists() and not args.output.resolve().is_relative_to(args.root)
    root = args.root; execution = read(root/'execution.json'); revision = execution['source']
    integer(execution['native_exit_code'], 0)
    before, after = read(root/'source-before.json'), read(root/'source-after.json')
    assert before == after and before['revision'] == revision
    original_inventory = fixture_archive.inventory(root)
    for name, digest in before['files'].items():
        assert hashlib.sha256(subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+name], cwd=REPO)).hexdigest() == digest
        assert sha(root/'selected-source'/name) == digest == sha(REPO/name)
    assert (root/'executed-producer.py').read_bytes() == (root/'selected-source/scripts/run-state-creation-native.py').read_bytes()
    external = read(root/'external-source-before.json'); assert external == read(root/'external-source-after.json')
    captured = read(root/'external-captured-paths.json'); assert set(external) == set(captured)
    for name, digest in external.items():
        assert sha(root/captured[name]) == digest == sha(name)
    for value in read(root/'generated-inputs.json').values():
        assert sha(root/value['captured']) == value['sha256']
    sdk, binary, command = read(root/'actual-sdk.json'), read(root/'binary.json'), read(root/'commands.json')
    assert sdk['admission']['stable_identity_observed_twice'] is True
    assert sdk['args'] == command['run'] == [str(root/'integrity-race.test'), '-test.v', '-test.run=^TestStateCreationNativeRecovery$', '-test.count=1', '-test.timeout=2m']
    assert sdk['exe'] == str(root/'integrity-race.test') and sdk['working_directory'] == command['cwd'] == str(REPO)
    assert sdk['environment'] == command['environment'] == dict(GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='', WF_STATE_CREATION_ROOT=str(root/'fixture'))
    assert sdk['exe_sha256'] == binary['sha256'] == sha(root/'integrity-race.test')
    assert 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info'] and '-race=true' in binary['build_info']
    assert re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s', binary['build_info'])
    integer(sdk['pid'], int(sdk['stat'].split()[0]))
    assert sdk['start_ticks'] == sdk['stat'].rsplit(') ',1)[1].split()[19]
    try:
        current = Path('/proc', str(sdk['pid']), 'stat').read_text().rsplit(') ',1)[1].split()[19]
        assert current != sdk['start_ticks'], 'original SDK incarnation still live'
    except FileNotFoundError:
        pass
    log = (root/'native.log').read_text()
    assert log.rstrip().endswith('PASS') and not any(x in log for x in ['DATA RACE', '--- FAIL:', '--- SKIP:'])
    assert re.findall(r'^--- PASS: (\w+) \(', log, re.M) == ['TestStateCreationNativeRecovery']
    assert log.count('native state creation recovered: attempts=2 included_keys=1024 peers=3 consumers=0 elapsed=') == 1
    properties = subprocess.check_output(['systemctl', '--user', 'show', args.service, '-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'], text=True)
    fields = dict(line.split('=',1) for line in properties.splitlines())
    assert fields['InvocationID'] == args.invocation and fields['ExecMainPID'] == str(args.producer_pid)
    assert fields['MainPID'] == fields['ExecMainStatus'] == '0' and fields['Result'] == 'success'
    closure = closed.verify_no_open_originals(root)
    result = validate_fixture(root/'fixture')
    meta = read(args.metadata); inventory_path = args.metadata.parent/meta['inventory_file']
    inventory = read(inventory_path); assert sha(inventory_path) == meta['inventory_sha256']
    assert meta['all_archive_members_read_back'] is True and meta['all_current_fixture_files_unchanged_after_capture'] is True
    expected = dict(bytes=meta['archive_bytes'], sha256=meta['archive_sha256'])
    with args.archive.open('rb') as stream:
        declared, actual = fixture_archive.verify_hashed_stream(stream, expected)
    assert declared == inventory and original_inventory == inventory['files'] == fixture_archive.inventory(root)
    result.update(source=revision, actual_sdk_pid=sdk['pid'], actual_sdk_sha256=sdk['exe_sha256'], selected_git_inputs=len(before['files']), external_inputs=len(external),
                  original_unit_properties=properties, closure=closure, complete_archive=actual, members=meta['members'], native_component_accepted=True,
                  qualifies_24h=False, qualifies_full_matrix=False, original_failed_24h_verdict_unchanged=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
