#!/usr/bin/env python3
"""Verify retained wire bytes for the explicit protobuf-to-JSON worker profile."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

REPO = Path(__file__).resolve().parents[1]


def check(root, report):
    profile = json.loads((root/'journal-rollout.json').read_text())
    if profile['profile'] != 'protobuf-to-json' or profile['all_retained_wire_formats_verified'] is not True:
        raise ValueError('invalid journal rollout admission')
    codec = root/'journal-rollout-codec'
    codec.mkdir(exist_ok=True)
    schema = REPO/'protocol/v1/journal.proto'
    subprocess.run(['protoc', '--proto_path='+str(REPO), '--python_out='+str(codec),
                    'protocol/v1/journal.proto'], check=True)
    generated = codec/'protocol/v1/journal_pb2.py'
    spec = importlib.util.spec_from_file_location('rollout_journal_pb2', generated)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sessions = json.loads((root/'process-evidence.json').read_text())
    owners = {s['worker_id']: s for s in sessions}
    if len(owners) != len(sessions):
        raise ValueError('duplicate rollout worker identity')
    for owner, session in owners.items():
        identity = re.fullmatch(r'matrix-process-(\d+)-generation-(\d+)', owner)
        if not identity:
            raise ValueError('unknown rollout worker')
        encoding = 'protobuf-v1' if int(identity[2]) == 0 else 'json'
        admission = json.loads((root/(owner+'-encoding.json')).read_text())
        if admission != {'pid': session['pid'], 'worker': owner, 'encoding': encoding}:
            raise ValueError('worker encoding admission disagrees')
    proofs = sorted(root.glob('rollout-*.json'))
    if len(proofs) != report['invocations']:
        raise ValueError('missing invocation wire proofs')
    seen = set()
    mixed = protobuf = json_entries = 0
    for path in proofs:
        proof = json.loads(path.read_text())
        identity = (proof['type'], proof['id'])
        if identity in seen:
            raise ValueError('duplicate wire proof')
        seen.add(identity)
        proto_count = json_count = 0
        sequences = set()
        for record in proof['records']:
            sequence = record['sequence']
            if type(sequence) is not int or sequence <= 0 or sequence in sequences:
                raise ValueError('invalid raw sequence')
            sequences.add(sequence)
            wire = base64.b64decode(record['wire_base64'], validate=True)
            is_proto = wire.startswith(b'WFJ\0')
            if is_proto:
                decoded = module.JournalRecord()
                decoded.ParseFromString(wire[4:])
                if decoded.version != 1 or decoded.sequence != 0 or not decoded.HasField('entry'):
                    raise ValueError('invalid stored protobuf envelope')
                owner = decoded.entry.worker_id
                kind = decoded.entry.kind
            else:
                decoded = json.loads(wire)
                owner = decoded.get('worker_id', '')
                kind = decoded['kind']
            if not owner:
                if is_proto or kind != 'Started':
                    raise ValueError('unattributed worker record')
                continue
            if owner not in owners:
                raise ValueError('record has no admitted worker')
            generation = int(owner.rsplit('-generation-', 1)[1])
            if is_proto != (generation == 0):
                raise ValueError('actual writer format disagrees with generation')
            if is_proto:
                proto_count += 1
            else:
                json_count += 1
        observed_mixed = proto_count > 0 and json_count > 0
        if (proof['protobuf_worker_entries'], proof['json_worker_entries'], proof['mixed_worker_entries']) != (proto_count, json_count, observed_mixed):
            raise ValueError('wire counts disagree')
        mixed += int(observed_mixed)
        protobuf += proto_count
        json_entries += json_count
    if mixed == 0 or profile['mixed_invocations'] != mixed:
        raise ValueError('no verified mixed-writer recovery')
    return dict(profile='protobuf-to-json', invocations=len(proofs), mixed_invocations=mixed,
                protobuf_worker_entries=protobuf, json_worker_entries=json_entries,
                schema_sha256=hashlib.sha256(schema.read_bytes()).hexdigest(),
                generated_codec_sha256=hashlib.sha256(generated.read_bytes()).hexdigest(),
                all_retained_wire_bytes_independently_decoded=True,
                qualifies_default_writer_profile=False, clears_full_tier3_release=False)
