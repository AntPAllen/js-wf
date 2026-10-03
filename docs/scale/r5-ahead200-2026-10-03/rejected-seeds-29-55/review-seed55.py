#!/usr/bin/env python3
"""Review the seed55 retry timeline directly from preserved, hashed originals."""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
manifest = json.loads((a.root / 'manifest.json').read_text())['files']
with tarfile.open(a.root / 'originals.tar.gz') as archive:
    originals = {m.name: archive.extractfile(m).read() for m in archive.getmembers() if m.isfile()}
assert {name: hashlib.sha256(data).hexdigest() for name, data in originals.items()} == manifest
base = 'seed-55/tier3-mixed-journal/'
def read(name): return json.loads(originals[base + name])
identity = 'tier3-55-batch-1-2'
receipts = [x for x in read('controller-receipts.json') if x['subject'] == 'wf.jrn.matrixtimer.' + identity]
operations = [x for x in read('controller-operations.json') if x.get('ID') == identity]
dispatch = [x for x in read('dispatch.json') if x.get('ID') == identity]
tail = max(receipts, key=lambda x: x['entry']['index'])
assert tail['sequence'] == 589 and tail['entry']['index'] == 16 and tail['entry']['kind'] == 'StepRequested'
assert tail['entry']['payload']['name'] == 'timer-5'
hint = [x for x in operations if x['Operation'] == 'timer_native_hint' and x['JournalIndex'] == 16]
assert len(hint) == 1
hint = hint[0]
assert hint['Error'] == 'context deadline exceeded' and hint['TimerPublished'] is False
retry = [x for x in dispatch if x['Stage'] == 'execution_retry' and x['RunSequence'] == 216]
assert len(retry) == 1 and retry[0]['Error'] == 'timer schedule was not confirmed: context deadline exceeded'
sequence = [x for x in dispatch if x['RunSequence'] == 216]
assert [x['Stage'] for x in sequence] == ['fetched', 'lease_acquired', 'execution_retry', 'nak', 'released']
assert sequence[-2]['Error'] == '' and sequence[-1]['Error'] == ''
assert not [x for x in dispatch if x['Stage'] == 'fetched' and x['At'] > retry[0]['At']]
latest_receipt = max(read('controller-receipts.json'), key=lambda x: x['sequence'])
assert latest_receipt['sequence'] == 608
assert latest_receipt['observed_at'] < '2026-10-02T22:59:13Z'
events_name = 'seed-55/tier3-mixed-journal-events.jsonl'
events = [json.loads(line) for line in originals[events_name].splitlines()]
failures = [x for x in events if 'no provable pending timer for clock cut: context deadline exceeded' in x.get('Output', '')]
assert len(failures) == 1
names = ['controller-receipts.json', 'controller-operations.json', 'dispatch.json', 'faults.json']
physical = []
for node in range(5):
    name = f'fault-2-failure-node{node}-jetstream.json'
    names.append(name)
    snapshot = read(name)
    streams = [s for account in snapshot['data']['account_details']
               for s in account['stream_detail'] if s['name'] == 'WF_RUN']
    assert len(streams) == 1
    stream = streams[0]
    assert stream['state']['messages'] == 1 and stream['state']['first_seq'] == 216
    assert stream['state']['first_ts'] == '2026-10-02T22:59:43.488253032Z'
    consumers = [c for c in stream['consumer_detail'] if c['name'] == 'WF_P_43']
    assert len(consumers) == 1
    consumer = consumers[0]
    assert consumer['num_ack_pending'] == 1 and consumer['num_pending'] == 0
    assert consumer['num_redelivered'] == 0 and consumer['ack_floor']['stream_seq'] == 211
    assert consumer['cluster']['leader'].endswith('-n1')
    if node == 1:
        assert consumer['cluster']['leader_since'] == '2026-10-02T22:58:48.213871347Z'
        assert len(consumer['cluster']['replicas']) == 4
        assert all(r['current'] for r in consumer['cluster']['replicas'])
        assert stream['state']['first_ts'] > snapshot['after']
    physical.append(dict(node=node, before=snapshot['before'], after=snapshot['after'],
                         stream_state=stream['state'], consumer=consumer))
report = dict(run_id=37040118844, revision='52f4e51ba190e24132933de967d5317c481b21fd',
              verified_archive_members=len(originals), seed=55, identity=identity,
              final_receipt=tail, failed_native_hint=hint, retry_delivery=sequence,
              latest_any_receipt=latest_receipt, admission_failure=failures[0],
              all_five_physical_pending_snapshots=physical,
              source_hashes={base+n: manifest[base+n] for n in names} | {events_name: manifest[events_name]},
              later_fetch_observed=False, actual_nak_application_confirmed=False,
              server_cause_confirmed=False, qualifies_seed=False,
              next_boundary='Retained StepRequested plus NAK during skewed leader transition; no subsequent fetch observed before cancellation')
a.output.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(dict(verified_archive_members=len(originals), final_index=16, run_sequence=216,
                      later_fetch_observed=False, server_cause_confirmed=False)))
