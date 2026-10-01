#!/usr/bin/env python3
"""Explain recorded repair/fencing decisions; never certify the full soak."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path

FENCING = {
    'lease_initialization_lost': 'Lease initialization did not confirm ownership; the delivery retries.',
    'lease_heartbeat_lost': 'Heartbeat renewal could not confirm continuing ownership and returned ErrLost; processing was canceled.',
    'lease_execution_lost': 'Execution renewal could not confirm ownership and returned ErrLost; the worker stops this delivery.',
    'journal_stale': 'The journal rejected this owner’s epoch or expected tail; the delivery retries.',
    'lease_release_lost': 'Lease cleanup returned ErrLost; cleanup must preserve successor safety.',
}
REPAIRS = {
    ('start', 'missing_journal'): 'A retained invocation had no journal; the scanner attempted a deduplicated start wakeup.',
    ('signal', 'unconsumed_signal_nonterminal_generation'): 'A retained signal was unconsumed in a nonterminal journal and matched the current invocation generation; the scanner attempted a signal wakeup.',
    ('suspended', 'signal'): 'A suspended signal wait had an available signal; the scanner attempted a wakeup using its journal tail and retry window.',
    ('suspended', 'timer'): 'A suspended timer wait was due; the scanner attempted a wakeup using its journal tail and retry window.',
    ('suspended', 'select'): 'At least one case of a suspended select was ready; the scanner attempted a wakeup using its journal tail and retry window.',
    ('suspended', 'continuation'): 'A committed continuation was ready to resume; the scanner attempted a wakeup using its journal tail and retry window.',
}


def stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('event timestamp lacks a timezone')


def check(metrics, fencing, repairs):
    if not isinstance(metrics, list) or len(metrics) != 5:
        raise ValueError('expected five worker metric records')
    expected = {f'tier3-mixed-{i}': m['fencing_events'] for i, m in enumerate(metrics)}
    observed = Counter()
    explanations = []
    for index, event in enumerate(fencing or []):
        reason = event.get('Reason')
        if reason not in FENCING or event.get('Worker') not in expected:
            raise ValueError('unknown fencing reason or worker')
        if not all(event.get(k) for k in ('At', 'Type', 'ID', 'Error', 'RunSequence', 'Delivery')):
            raise ValueError('missing fencing evidence')
        if reason != 'lease_initialization_lost' and event.get('Epoch', 0) <= 0:
            raise ValueError('missing fenced ownership epoch')
        stamp(event['At'])
        observed[event['Worker']] += 1
        explanations.append(dict(kind='fencing', index=index, evidence=event, explanation=FENCING[reason]))
    if any(observed[worker] != count for worker, count in expected.items()):
        raise ValueError('fencing records disagree with worker counters')
    repair_counts = Counter()
    for index, event in enumerate(repairs or []):
        key = (event.get('kind'), event.get('reason'))
        outcome = event.get('outcome')
        if key not in REPAIRS or outcome not in ('acknowledged', 'uncertain', 'dry_run'):
            raise ValueError('unknown repair reason or outcome')
        if not all(event.get(k) for k in ('at', 'type', 'id')):
            raise ValueError('missing repair invocation or timestamp')
        stamp(event['at'])
        if (outcome == 'uncertain') != bool(event.get('error')):
            raise ValueError('repair publication uncertainty was erased or invented')
        if event['kind'] == 'suspended':
            if event.get('journal_sequence', 0) <= 0 or (outcome != 'dry_run' and event.get('retry_window', 0) <= 0):
                raise ValueError('missing suspended journal tail or retry window')
        elif event.get('source_sequence', 0) <= 0 or event.get('invocation_sequence', 0) <= 0:
            raise ValueError('missing repair source or invocation generation')
        suffix = {
            'acknowledged': ' Publication was acknowledged; message-ID deduplication can make this a repeated wakeup.',
            'uncertain': ' Publication did not return a successful acknowledgment; it may have committed and needs a safe retry.',
            'dry_run': ' This was a dry-run decision; no publication was attempted.',
        }[outcome]
        explanations.append(dict(kind='repair', index=index, evidence=event, explanation=REPAIRS[key]+suffix))
        repair_counts[f'{event["kind"]}/{outcome}'] += 1
    return dict(scope='recorded decisions of five R5 workers and start/signal/suspended reconcilers',
                fencing_records=sum(observed.values()), repair_records=sum(repair_counts.values()),
                fencing_per_worker=dict(observed), repairs_by_outcome=dict(repair_counts),
                explanations=explanations, server_root_causes_confirmed=False,
                independently_reaudits_server_state=False, clears_full_tier3_release=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    names = ('worker-metrics.json', 'fencing.json', 'repairs.json')
    blobs = [(args.root/name).read_bytes() for name in names]
    result = check(*(json.loads(blob) for blob in blobs))
    result['input_sha256'] = {name: hashlib.sha256(blob).hexdigest() for name, blob in zip(names, blobs)}
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'explanations'}, indent=2))
