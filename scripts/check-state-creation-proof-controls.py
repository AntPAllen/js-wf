#!/usr/bin/env python3
"""Reject substitutions in an actual native creation proof using fresh copies only."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('creation_review', REPO/'scripts/review-state-creation-native.py')
review = importlib.util.module_from_spec(spec); spec.loader.exec_module(review)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists() and not args.output.resolve().is_relative_to(args.fixture.resolve())
    positive = review.validate_fixture(args.fixture)
    original = {name:(args.fixture/name).read_bytes() for name in ['native-proof.json', 'expected-values.json', 'wire.jsonl']}
    proof = json.loads(original['native-proof.json'])
    mutations = [
        ('success-false', lambda p:p.update(passed=False)),
        ('success-integer', lambda p:p.update(passed=1)),
        ('three-attempts', lambda p:p.update(attempts=3)),
        ('fractional-attempts', lambda p:p.update(attempts=2.0)),
        ('extended-parent-budget', lambda p:p.update(parent_budget_ns=30_000_000_000)),
        ('stage-over-deadline', lambda p:p.update(elapsed_ns=21_000_000_000)),
        ('early-creation-return', lambda p:p.update(first_creation_elapsed_ns=1_000_000_000)),
        ('parent-error-substitution', lambda p:p.update(first_native_error='context deadline exceeded')),
        ('held-request-forwarded', lambda p:p['pending_before_close'].update(forwarded_bytes=1)),
        ('unjoined-held-request', lambda p:p['pending_after_close'].update(disposition='held')),
        ('wrong-request-stream', lambda p:p['pending_before_close'].update(subject='$JS.API.CONSUMER.CREATE.KV_OTHER.c')),
        ('truncated-wire', lambda p:p['proxy_trace'].update(truncated=True)),
        ('wire-record-count', lambda p:p['proxy_trace'].update(frame_records=p['proxy_trace']['frame_records']+1)),
        ('client-byte-count', lambda p:p['proxy_stats'].update(client_to_server=p['proxy_stats']['client_to_server']+1)),
        ('active-relay', lambda p:p['proxy_stats'].update(active_connections=1)),
        ('false-native-barrier', lambda p:p['snapshot_observations'][5].update(initial_complete=False)),
        ('incomplete-native-set', lambda p:p['snapshot_observations'][5].update(included=1025)),
        ('changed-snapshot-deadline', lambda p:p['snapshot_observations'][4].update(deadline=p['first_creation_started'])),
        ('leaked-native-consumer', lambda p:p['state_stream_info']['state'].update(consumer_count=1)),
        ('noncurrent-replica', lambda p:p['state_stream_info']['cluster']['replicas'][0].update(current=False)),
        ('duplicate-peer', lambda p:p['server_ids'].__setitem__(1,p['server_ids'][0])),
    ]
    rejected = []
    with tempfile.TemporaryDirectory(prefix='js-wf-state-creation-proof-controls-') as temporary:
        root = Path(temporary)
        for name, data in original.items():
            (root/name).write_bytes(data)
        for name, mutate in mutations:
            changed = copy.deepcopy(proof); mutate(changed)
            (root/'native-proof.json').write_text(json.dumps(changed))
            try:
                review.validate_fixture(root)
            except (AssertionError, ValueError, KeyError, TypeError):
                rejected.append(name)
            else:
                raise AssertionError('accepted proof substitution: '+name)
        values = json.loads(original['expected-values.json']); values.pop('included-0000')
        changed_values = json.dumps(values).encode()
        changed = copy.deepcopy(proof); changed['expected_values_sha256'] = hashlib.sha256(changed_values).hexdigest()
        (root/'expected-values.json').write_bytes(changed_values)
        (root/'native-proof.json').write_text(json.dumps(changed))
        try:
            review.validate_fixture(root)
        except (AssertionError, ValueError, KeyError, TypeError):
            rejected.append('self-consistent-incomplete-payload-corpus')
        else:
            raise AssertionError('accepted self-consistent incomplete payload corpus')
    assert all((args.fixture/name).read_bytes() == data for name,data in original.items())
    assert review.validate_fixture(args.fixture) == positive
    report = dict(actual_positive_accepted=True, original_bytes_unchanged=True, rejected_count=len(rejected), rejected=rejected,
                  scope='Raw native report/wire/payload substitutions only; full source/SDK/unit/archive checks are in the independent terminal reviewer.')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
