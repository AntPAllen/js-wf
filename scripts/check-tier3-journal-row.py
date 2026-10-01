#!/usr/bin/env python3
"""Verify one executed R5 mixed journal row; never certify full Tier 3 release."""
import argparse
import importlib.util
import json
from pathlib import Path
import re


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


matrix = load('matrix_row', 'check-matrix-campaign.py')
execution = load('matrix_execution', 'check-matrix-result.py')
TESTS = {'journal': 'TestFiveContainerMixedJournalLeaderEveryThirtySeconds',
         'consumer': 'TestFiveContainerMixedConsumerLeaderEveryThirtySeconds'}
TEST = TESTS['journal']


def check(events, duration, expected_row='journal'):
    test = TESTS[expected_row]
    execution.check(events, test, duration)
    log = ''.join(e.get('Output', '') for e in events if e.get('Test') == test)
    row, seed, found_duration, replicas, batches, invocations, entries, faults, release = matrix.one(
        r'TIER3_MIXED_RESULT row=(\w+) seed=(\d+) duration=(\S+) five_replicas=(\w+) '
        r'batches=(\d+) invocations=(\d+) entries=(\d+) faults=(\d+) full_matrix_release=(\w+)', log)
    seconds = {'35s': 35, '10m': 600}[duration]
    if row != expected_row or matrix.seconds(found_duration) != seconds or replicas != 'true' or release != 'false':
        raise ValueError('incorrect row, duration, replica scope or release claim')
    batches, invocations, entries, faults = map(int, (batches, invocations, entries, faults))
    if batches < 1 or invocations != batches*28 or entries <= invocations or faults != (seconds-1)//30:
        raise ValueError('incomplete workload, audit entries or fault count')
    active_consumer_faults = None
    if row == 'consumer':
        selections = re.findall(r'TIER3_CONSUMER_FAULT consumer=(WF_P_\d{2}) node=(\d+) pending=(\d+) ack_pending=(\d+)', log)
        if len(selections) != faults or any(int(node) >= 5 for _, node, _, _ in selections):
            raise ValueError('missing or invalid confirmed consumer selections')
        active_consumer_faults = sum(int(pending) > 0 or int(acks) > 0 for _, _, pending, acks in selections)
        if active_consumer_faults == 0:
            raise ValueError('consumer row never targeted an observed active delivery')
    cells = {}
    for typ, per_batch in zip(matrix.TYPES, (4, 3, 2, 1, 6, 12)):
        count, terminal, progress = matrix.one(
            r'TIER3_MIXED_CELL type=' + typ + r' invocations=(\d+) terminal_p99=(\S+) progress_p99=(\S+)', log)
        terminal, progress = matrix.seconds(terminal), matrix.seconds(progress)
        if int(count) != batches*per_batch or terminal >= 30 or progress >= 30:
            raise ValueError(f'{typ}: incorrect count or over-budget p99')
        cells[typ] = dict(invocations=int(count), terminal_p99_seconds=terminal, progress_p99_seconds=progress)
    return dict(scope=f'single five-container R5 {row}-leader row', seed=int(seed), duration_seconds=seconds,
                shortened_smoke=duration == '35s', invocations=invocations, journal_entries=entries,
                confirmed_faults=faults, active_consumer_faults=active_consumer_faults, cells=cells, clears_full_tier3_release=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--row', choices=tuple(TESTS), default='journal')
    parser.add_argument('--events', required=True, type=Path)
    parser.add_argument('--duration', required=True, choices=('35s', '10m'))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    events = [json.loads(line) for line in args.events.read_text().splitlines() if line.strip()]
    report = check(events, args.duration, args.row)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
