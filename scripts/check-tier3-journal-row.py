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
         'restart': 'TestFiveContainerMixedAllServersEveryThirtySeconds',
         'fanout_restart': 'TestFiveContainerMixedFanoutRestartEveryThirtySeconds',
         'route_quorum': 'TestFiveContainerMixedRouteQuorumEveryThirtySeconds'}
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
        if row == 'route_quorum':
            raw_t,raw_p=matrix.one(r'TIER3_ROUTE_RAW_CELL type='+typ+r' terminal_p99=(\S+) progress_p99=(\S+)',log)
            cells[typ]['raw_terminal_p99_seconds']=matrix.seconds(raw_t)
            cells[typ]['raw_progress_p99_seconds']=matrix.seconds(raw_p)
    scope = ('single five-container R5 quorum-removing route row' if row == 'route_quorum'
             else 'single five-container R5 mid-fan-out all-server restart row' if row == 'fanout_restart'
             else 'single five-container R5 all-server SIGKILL/restart row' if row == 'restart'
             else f'single five-container R5 {row}-leader row')
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


def check_fanout_cut_artifacts(root, report):
    check_restart_artifacts(root, report)
    faults = json.loads((root/'faults.json').read_text())
    for index, fault in enumerate(faults, 1):
        cut = json.loads((root/f'fault-{index}-fanout-cut.json').read_text())
        original = cut['ParentPrefix']
        recovered = json.loads((root/f'fault-{index}-fanout-recovered-prefix.json').read_text())
        selected = cut['Fault']
        children, pending = fault['fanout_children'], fault['fanout_pending_children']
        requests = [x['payload']['child_id'] for x in original
                    if x['kind']=='StepRequested' and x.get('payload',{}).get('kind')=='call_async']
        if not original or original[-1]['kind']!='Suspended' or original[-1]['sequence']!=fault['fanout_tail']:
            raise ValueError('restart cut lacks a suspended parent at recorded tail')
        if len(children)!=6 or len(set(children))!=6 or requests!=children or not pending or not set(pending)<=set(children):
            raise ValueError('restart cut lacks exactly six children and unfinished work')
        for key in ('fanout_parent','fanout_tail','fanout_children','fanout_pending_children'):
            if selected[key]!=fault[key]: raise ValueError('fanout cut disagrees with fault identity')
        prefixes = cut['ChildPrefixes']
        if set(prefixes)!=set(children): raise ValueError('missing child prefixes at restart cut')
        actual_pending = {child for child,records in prefixes.items()
                          if not records or records[-1]['kind'] not in ('Completed','Failed')}
        if actual_pending!=set(pending): raise ValueError('cut does not prove the recorded unfinished children')
        observed,killed = [datetime.fromisoformat(s.replace('Z','+00:00')) for s in (cut['ObservedAt'],fault['killed'])]
        if observed.tzinfo is None or observed>killed: raise ValueError('fanout cut was not observed before the kill')
        if recovered[:len(original)]!=original: raise ValueError('restart changed the exact parent journal prefix')
        final = json.loads((root/f'fault-{index}-fanout-final.json').read_text())
        def terminal_children(records, expected):
            if not records or records[-1]['kind']!='Completed': raise ValueError('fanout tree is not completed')
            ids=[x['payload']['child_id'] for x in records if x['kind']=='StepRequested' and x.get('payload',{}).get('kind')=='call_async']
            if len(ids)!=expected or len(set(ids))!=expected: raise ValueError('incorrect terminal child cardinality')
            return set(ids)
        if terminal_children(final['Parent'],6)!=set(children) or set(final['Children'])!=set(children):
            raise ValueError('final parent tree changed child identities')
        grandchildren=set()
        for records in final['Children'].values():
            ids=terminal_children(records,2)
            if grandchildren & ids: raise ValueError('duplicate grandchild identity across children')
            grandchildren |= ids
        if set(final['Grandchildren'])!=grandchildren: raise ValueError('final tree lacks expected grandchildren')
        for records in final['Grandchildren'].values():
            if not records or records[-1]['kind']!='Completed': raise ValueError('grandchild is not completed')
    return dict(unfinished_six_child_cuts=len(faults), recovered_prefixes=len(faults))


def timestamp_ns(value):
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None: raise ValueError('timezone missing')
    fraction=re.search(r'\.(\d{1,9})(?:Z|[+-]\d{2}:\d{2})$',value)
    return int(dt.replace(microsecond=0).timestamp())*1_000_000_000 + int((fraction[1] if fraction else '').ljust(9,'0'))


def check_route_artifacts(root, report):
    faults=json.loads((root/'faults.json').read_text())
    if len(faults)!=report['confirmed_faults']:raise ValueError('route fault count disagrees')
    for index,fault in enumerate(faults,1):
        nodes=fault['nodes']
        if len(nodes)!=3 or len(set(nodes))!=3 or any(n not in range(5) for n in nodes):raise ValueError('route cut does not remove R5 quorum')
        obs=json.loads((root/f'fault-{index}-route-observations.json').read_text())
        expected=[('isolated',n) for n in nodes]+[('reconnected',n) for n in range(5)]
        if [(x['Phase'],x['Node']) for x in obs]!=expected:raise ValueError('missing route cut/heal observations')
        if any(x['Routes']!=0 for x in obs[:3]) or any(x['Routes']<16 for x in obs[3:]):raise ValueError('routes do not confirm isolation/full heal')
        times=[timestamp_ns(fault['killed'])]+[timestamp_ns(x['At']) for x in obs]+[timestamp_ns(fault['healed'])]
        if times!=sorted(times):raise ValueError('route timestamps are reversed')
        probe=json.loads((root/f'fault-{index}-quorum-probe.json').read_text())
        if probe['Before']<=0 or not probe['UnacknowledgedError']:raise ValueError('missing observed no-quorum publication result')
    raw=json.loads((root/'latencies.json').read_text())
    adjusted=json.loads((root/'route-recovery-latencies.json').read_text())
    if not raw or len(raw)!=len(adjusted):raise ValueError('missing recovery samples')
    for sample,recovery in zip(raw,adjusted):
        enabled,observed=timestamp_ns(sample['enabled']),timestamp_ns(sample['observed'])
        baseline=enabled
        for fault in faults:
            killed,healed=timestamp_ns(fault['killed']),timestamp_ns(fault['healed'])
            if observed>=killed and enabled<=healed:baseline=max(baseline,healed)
        expected=max(0,observed-baseline)
        if sample['delay_ns']!=observed-enabled or recovery!={**sample,'delay_ns':expected}:
            raise ValueError('raw/recovery samples do not follow enabling/heal timestamps')
    for typ,cell in report['cells'].items():
        if sum(x['type']==typ and x['event']=='terminal' for x in raw)!=cell['invocations']:
            raise ValueError('route terminal samples disagree with workload count')
        for event,key in [('terminal','terminal'),('progress','progress')]:
            for samples,prefix in [(raw,'raw_'),(adjusted,'')]:
                values=sorted(x['delay_ns'] for x in samples if x['type']==typ and (x['event']=='terminal')==(event=='terminal'))
                if not values:raise ValueError('missing route workload samples')
                p99=values[(99*len(values)+99)//100-1]/1e9
                if abs(p99-cell[prefix+key+'_p99_seconds'])>1e-6:raise ValueError('route p99 disagrees with original samples')
    return dict(quorum_removing_cuts=len(faults),matched_recovery_samples=len(raw),basis='later of enabling event and last overlapping confirmed route/R5 heal')


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
    if args.row in ('restart','fanout_restart'):
        if args.root is None: parser.error('--root is required for the restart row')
        report['restart_artifact_checks'] = check_restart_artifacts(args.root, report)
    if args.row == 'fanout_restart':
        report['fanout_cut_artifact_checks'] = check_fanout_cut_artifacts(args.root, report)
    if args.row == 'route_quorum':
        if args.root is None: parser.error('--root is required for the route row')
        report['route_artifact_checks'] = check_route_artifacts(args.root, report)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
