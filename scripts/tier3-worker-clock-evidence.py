#!/usr/bin/env python3
"""Worker-clock proof and diagnostic timestamp rules for the pending R5 row.

Correct only the deliberate Go wall-clock overlay. Broker/controller timestamps
and original child records remain unchanged; sampled network delay is not a
clock correction. This module alone does not qualify an executed row.
"""
import base64
import copy
from datetime import datetime, timezone
import json
import re

SECOND = 1_000_000_000
OFFSETS = {f'matrix-process-{i}-generation-0': offset * SECOND
           for i, offset in enumerate((5, -5, 0, 5, -5))}


def timestamp_ns(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('invalid clock timestamp')
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    fraction = re.search(r'\.(\d+)', value)
    return int(dt.replace(microsecond=0).timestamp()) * SECOND + int(
        (fraction[1] if fraction else '').ljust(9, '0'))


def corrected_timestamp(value, offset):
    seconds, nanos = divmod(timestamp_ns(value) - offset, SECOND)
    date = datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    fraction = ('.' + f'{nanos:09d}'.rstrip('0')) if nanos else ''
    return date + fraction + 'Z'


def normalize_records(records):
    """Return copies; only At changes, by the fixed overlay assigned to Worker."""
    result = copy.deepcopy(records or [])
    for event in result:
        if event.get('Worker') not in OFFSETS:
            raise ValueError('unknown worker in clock diagnostic records')
        event['At'] = corrected_timestamp(event['At'], OFFSETS[event['Worker']])
    return result


def check_normalized_records(raw, normalized):
    if normalized != normalize_records(raw):
        raise ValueError('normalized diagnostics differ from fixed-offset original records')
    return len(raw or [])


def check_probe(proof):
    """Check all five samples against retained actual GetMsg replies at observation."""
    observed = timestamp_ns(proof['observed'])
    samples, messages = proof['samples'], proof['messages']
    if len(samples) != 5 or len(messages) != 5 or {s['worker'] for s in samples} != set(OFFSETS):
        raise ValueError('clock proof must cover each of the original five workers')
    sequences = set()
    for sample, message in zip(samples, messages):
        worker = sample['worker']
        sequence = sample['sequence']
        if type(sequence) is not int or sequence <= 0 or sequence in sequences:
            raise ValueError('invalid or reused broker clock sequence')
        sequences.add(sequence)
        if type(message['sequence']) is not int or message['sequence'] != sequence or message['subject'] != 'matrix.clock.' + worker:
            raise ValueError('broker clock message identity differs from sample')
        server = timestamp_ns(sample['server_at'])
        if server != timestamp_ns(message['time']) or not observed - 4*SECOND <= server <= observed + SECOND:
            raise ValueError('clock message is stale, future, or differs from broker time')
        offset = timestamp_ns(sample['worker_at']) - server
        if type(sample['offset_ns']) is not int or sample['offset_ns'] != offset or abs(offset - OFFSETS[worker]) > SECOND:
            raise ValueError('worker clock is unshifted or has the wrong offset')
        payload = json.loads(base64.b64decode(message['data'], validate=True))
        if payload['worker'] != worker or timestamp_ns(payload['worker_at']) != timestamp_ns(sample['worker_at']):
            raise ValueError('published worker timestamp differs from sample')
        if payload.get('sequence', 0) != 0 or payload.get('offset_ns', 0) != 0:
            raise ValueError('published clock payload invents post-acknowledgment fields')
    return dict(workers=5, broker_clock_messages=5,
                deliberate_offsets_ns=OFFSETS.copy(),
                proves_full_row=False, confirms_server_root_cause=False)
