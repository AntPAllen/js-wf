#!/usr/bin/env python3
"""Verify one executed R5 mixed fault row; never certify full Tier 3 release."""
import argparse
from datetime import datetime
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
         'consumer': 'TestFiveContainerMixedConsumerLeaderEveryThirtySeconds',
         'restart': 'TestFiveContainerMixedAllServersEveryThirtySeconds'}
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
    scope = 'single five-container R5 all-server SIGKILL/restart row' if row == 'restart' else f'single five-container R5 {row}-leader row'
    return dict(scope=scope, seed=int(seed), duration_seconds=seconds,
                shortened_smoke=duration == '35s', invocations=invocations, journal_entries=entries,
                confirmed_faults=faults, active_consumer_faults=active_consumer_faults, cells=cells, clears_full_tier3_release=False)


def check_consumer_artifacts(root, report):
    faults = json.loads((root/'faults.json').read_text())
    if len(faults) != report['confirmed_faults']:
        raise ValueError('consumer fault artifact count disagrees')
    clusters, active = set(), 0
    for index, fault in enumerate(faults, 1):
        snapshot = json.loads((root/f'fault-{index}-consumer-before.json').read_text())
        info, node, partition = snapshot['Info'], snapshot['Node'], snapshot['Partition']
        cluster = info['cluster']
        if not 0 <= node < 5 or not 0 <= partition < 64:
            raise ValueError('invalid consumer target node or partition')
        if info['name'] != f'WF_P_{partition:02d}' or fault['consumer'] != info['name'] or fault['node'] != node:
            raise ValueError('consumer target identity disagrees with fault record')
        if cluster['leader'] != f"{cluster['name']}-n{node}" or len(cluster['replicas']) != 4:
            raise ValueError('fault target was not the observed R5 consumer leader')
        if info['num_pending'] != fault.get('pending', 0) or info['num_ack_pending'] != fault.get('ack_pending', 0):
            raise ValueError('consumer activity disagrees with selection snapshot')
        times = [datetime.fromisoformat(s.replace('Z','+00:00')) for s in (snapshot['ObservedAt'], fault['killed'], fault['healed'])]
        if any(t.tzinfo is None for t in times) or not times[0] <= times[1] <= times[2]:
            raise ValueError('missing or reversed consumer observation/kill/heal times')
        clusters.add(cluster['name'])
        active += info['num_pending'] > 0 or info['num_ack_pending'] > 0
    if len(clusters) != 1 or active != report['active_consumer_faults']:
        raise ValueError('consumer cluster or active fault coverage disagrees')
    return dict(cluster=clusters.pop(), matched_consumer_snapshots=len(faults), active_selections=active)


def check_restart_artifacts(root, report):
    faults = json.loads((root/'faults.json').read_text())
    if len(faults) != report['confirmed_faults']:
        raise ValueError('restart fault count disagrees')
    for index, fault in enumerate(faults, 1):
        operations = json.loads((root/f'fault-{index}-restart-operations.json').read_text())
        expected = [(action, node) for action in ('sigkill_removed', 'restarted') for node in range(5)]
        if [(x['Action'], x['Node']) for x in operations] != expected or fault['nodes'] != list(range(5)):
            raise ValueError('all five kills must precede all five restarts')
        times = [datetime.fromisoformat(s.replace('Z','+00:00')) for s in
                 [fault['killed']] + [x['At'] for x in operations] + [fault['healed']]]
        if any(t.tzinfo is None for t in times) or times != sorted(times):
            raise ValueError('restart operation times are missing or reversed')
    return dict(confirmed_all_down_boundaries=len(faults), nodes_per_boundary=5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, help='required consumer/restart fault artifacts')
    parser.add_argument('--row', choices=tuple(TESTS), default='journal')
    parser.add_argument('--events', required=True, type=Path)
    parser.add_argument('--duration', required=True, choices=('35s', '10m'))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    events = [json.loads(line) for line in args.events.read_text().splitlines() if line.strip()]
    report = check(events, args.duration, args.row)
    if args.row == 'consumer':
        if args.root is None: parser.error('--root is required for the consumer row')
        report['selection_artifact_checks'] = check_consumer_artifacts(args.root, report)
    if args.row == 'restart':
        if args.root is None: parser.error('--root is required for the restart row')
        report['restart_artifact_checks'] = check_restart_artifacts(args.root, report)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
