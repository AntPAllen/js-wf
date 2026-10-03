#!/usr/bin/env python3
"""Regenerate the real TCP journal recovery proof; reduced counts are diagnostic."""
import argparse
import base64
import hashlib
import json
import re
from pathlib import Path


def check(root, events_path, count=1000):
    if type(count) is not int or count < 2 or count > 1000 or count % 2:
        raise ValueError('invalid even case count')
    def read(name):
        return json.loads((root / name).read_text())
    events = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
    test = 'TestThousandJournalNetworkLostAckRecoveries'
    for name in (test, None):
        passes = [e for e in events if e.get('Package') == 'js-wf/integration'
                  and e.get('Test') == name and e.get('Action') == 'pass']
        if len(passes) != 1:
            raise ValueError('missing/duplicate actual test or package completion')
    if any(e.get('Action') in ('fail', 'build-fail', 'skip') for e in events):
        raise ValueError('failed/skipped execution')
    expected_summary = dict(cases=count, committed=count//2, absent=count//2, leader_kills=1,
                            cross_peer_checks=3*count, full_thousand=count == 1000)
    if read('result.json') != expected_summary:
        raise ValueError('incorrect full-size scope or case summary')
    if {p.name for p in root.glob('case-*.json')} != {f'case-{i:04d}.json' for i in range(count)}:
        raise ValueError('missing/extra case receipts')
    traffic = read('traffic.json')
    if traffic['truncated'] or [c['id'] for c in traffic['connections']] != list(range(1, count+1)):
        raise ValueError('incomplete TCP transcript')
    frames = {(i, direction): bytearray() for i in range(1, count+1)
              for direction in ('client_to_server', 'server_to_client')}
    for frame in traffic['frames']:
        frames[frame['connection'], frame['direction']] += base64.b64decode(frame['data'], validate=True)
    final = read('proxy-final.json')
    if (final['buffer_overflows'] != 0 or final['buffered_bytes'] != 0 or final['responses_held']
            or final['active_connections'] != 0):
        raise ValueError('invalid relay buffer or final connection state')
    for direction in ('client_to_server', 'server_to_client'):
        if sum(len(body) for (i, side), body in frames.items() if side == direction) != final[direction]:
            raise ValueError('TCP transcript disagrees with forwarded byte counts')
    receipts = []
    for i in range(count):
        case = read(f'case-{i:04d}.json')
        id = f'network-ack-{i:04d}'
        committed = i % 2 == 0
        if case['id'] != id or case['committed'] is not committed or not case['unknown'].startswith('journal append outcome unknown:'):
            raise ValueError('case identity, branch or ambiguous outcome mismatch')
        before, after = case['before'], case['after']
        # Per-case map cleanup can lag the closed socket. The complete trace
        # identifies actual connections; final counters must show full cleanup.
        if (after['buffer_overflows'] != 0 or before['responses_held']
                or after['server_to_client'] < before['server_to_client']):
            raise ValueError('response escaped fault or relay evidence invalid')
        subject = 'wf.jrn.test.'+id
        publish = b'HPUB '+subject.encode()+b' '
        payload = frames[i+1, 'client_to_server']
        if re.search(rb'"stream"\s*:\s*"WF_JRN"', frames[i+1, 'server_to_client']):
            raise ValueError('publish acknowledgment escaped the network fault')
        if payload.count(publish) != int(committed):
            raise ValueError('actual TCP publication disagrees with commit branch')
        if committed:
            if (after['held_bytes'] <= before['held_bytes']
                    or case['first'] != case['last'] or case['retry_sequence'] != 0):
                raise ValueError('committed acknowledgment loss or stale retry proof missing')
        elif (case['first'] is not None
              or case['retry_sequence'] != case['last']['Sequence']):
            raise ValueError('absent publication/retry proof missing')
        last = case['last']
        entry = json.loads(base64.b64decode(last['Data'], validate=True))
        if (last['Subject'] != subject or type(last['Sequence']) is not int
                or last['Sequence'] != i+1 or entry != dict(epoch=1, index=0, kind='Started', worker_id=id)
                or last['Header'].get('Nats-Expected-Last-Subject-Sequence') != ['0']):
            raise ValueError('retained entry identity, payload or sequence mismatch')
        if committed and (base64.b64decode(last['Data'], validate=True) not in payload
                          or b'Nats-Expected-Last-Subject-Sequence: 0\r\n' not in payload):
            raise ValueError('TCP publish payload/CAS differs from retained entry')
        receipts.append(last)
    for node in range(3):
        peer = read(f'peer-{node}.json')
        state = peer['state']
        if (peer['config']['name'] != 'WF_JRN' or peer['config']['num_replicas'] != 3
                or any(state[key] != count for key in ('messages', 'num_subjects', 'last_seq'))
                or state['first_seq'] != 1
                or read(f'peer-{node}-messages.json') != receipts):
            raise ValueError('cross-peer retained outcome or census differs')
    before, after = read('leader-before.json'), read('leader-after-kill.json')
    if (before['cluster']['leader'] == after['cluster']['leader']
            or not after['cluster']['leader'] or before['state']['messages'] != count//2):
        raise ValueError('midpoint leader move proof missing')
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.iterdir()) if p.is_file()}
    return dict(expected_summary, raw_input_sha256=hashes,
                events_sha256=hashlib.sha256(events_path.read_bytes()).hexdigest(),
                clears_full_thousand_network_gate=count == 1000,
                clears_full_release=False,
                scope='Real TCP cuts and one R3 journal leader restart; stores not retained/reopened. No full matrix or 24h qualification.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--events', type=Path, required=True)
    parser.add_argument('--cases', type=int, default=1000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output must be fresh')
    args.output.write_text(json.dumps(check(args.root, args.events, args.cases), indent=2)+'\n')
