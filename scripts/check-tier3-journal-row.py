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
TESTS = {'worker_isolation': 'TestFiveContainerMixedWorkerRepliesIsolatedFortyFiveSeconds',
         'worker_pause': 'TestFiveContainerMixedWorkerPausedFortyFiveSeconds',
         'worker_kill': 'TestFiveContainerMixedWorkerKilledEveryFiveSeconds',
         'journal': 'TestFiveContainerMixedJournalLeaderEveryThirtySeconds',
         'consumer': 'TestFiveContainerMixedConsumerLeaderEveryThirtySeconds',
         'restart': 'TestFiveContainerMixedAllServersEveryThirtySeconds',
         'fanout_restart': 'TestFiveContainerMixedFanoutRestartEveryThirtySeconds',
         'route_quorum': 'TestFiveContainerMixedRouteQuorumEveryThirtySeconds',
         'route_majority': 'TestFiveContainerMixedRouteMajorityEveryThirtySeconds'}
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
    expected_faults=(seconds-6)//60+1 if row in ('worker_pause','worker_isolation') else (seconds-1)//(5 if row=='worker_kill' else 30)
    if batches < 1 or invocations != batches*28 or entries <= invocations or faults != expected_faults:
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
    scope = ('single five-container R5 worker reply-isolation row' if row == 'worker_isolation'
             else 'single five-container R5 worker pause-past-lease row' if row == 'worker_pause'
             else 'single five-container R5 worker SIGKILL row' if row == 'worker_kill'
             else 'single five-container R5 majority-progress route row' if row == 'route_majority'
             else 'single five-container R5 quorum-removing route row' if row == 'route_quorum'
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


def check_majority_artifacts(root, report):
    faults=json.loads((root/'faults.json').read_text())
    if len(faults)!=report['confirmed_faults']:raise ValueError('majority fault count disagrees')
    for index,fault in enumerate(faults,1):
        nodes=fault['nodes']
        if len(nodes)!=1 or nodes[0] not in range(5):raise ValueError('majority row did not isolate exactly one server')
        obs=json.loads((root/f'fault-{index}-route-observations.json').read_text())
        if [(x['Phase'],x['Node']) for x in obs]!=[('isolated',nodes[0])]+[('reconnected',n) for n in range(5)]:raise ValueError('missing majority cut/heal observations')
        if obs[0]['Routes']!=0 or any(x['Routes']<16 for x in obs[1:]):raise ValueError('majority routes do not confirm cut/full heal')
        times=[timestamp_ns(fault['killed'])]+[timestamp_ns(x['At']) for x in obs]+[timestamp_ns(fault['healed'])]
        if times!=sorted(times):raise ValueError('reversed majority route times')
        progress=json.loads((root/f'fault-{index}-majority-progress.json').read_text())
        if progress['Before']<=0 or progress['After']<=progress['Before'] or fault['majority_sequence']!=progress['After']:
            raise ValueError('workflow journal did not advance during the partition')
        if progress['ClientURL']==progress['IsolatedURL']:raise ValueError('progress client is connected to the isolated server')
        if not timestamp_ns(obs[0]['At'])<=timestamp_ns(progress['ObservedAt'])<=timestamp_ns(obs[1]['At']):raise ValueError('journal progress was not observed during isolation')
        probe=json.loads((root/f'fault-{index}-quorum-probe.json').read_text())
        if probe['UnacknowledgedError'] or probe['AcknowledgedSequence']<=probe['Before']:raise ValueError('majority probe did not acknowledge new progress')
    return dict(confirmed_majority_progress_intervals=len(faults),basis='raw enabling-event latency; majority remains available')


def process_lines(path):
    data=path.read_bytes()
    end=data.rfind(b'\n')+1
    return [json.loads(line) for line in data[:end].splitlines() if line], end!=len(data)


def check_worker_artifacts(root,report):
    faults=json.loads((root/'faults.json').read_text())
    sessions=json.loads((root/'process-evidence.json').read_text())
    if len(faults)!=report['confirmed_faults'] or len(sessions)!=5+len(faults):
        raise ValueError('incomplete worker generations or kill count')
    by_worker={s['worker_id']:s for s in sessions}
    if len(by_worker)!=len(sessions) or len({s['pid'] for s in sessions})!=len(sessions):
        raise ValueError('duplicate worker generation or PID')
    retired=set()
    previous_schedule=None
    for fault in faults:
        scheduled=timestamp_ns(fault['scheduled'])
        if timestamp_ns(fault['killed'])-scheduled>=5_000_000_000 or (previous_schedule is not None and scheduled-previous_schedule!=5_000_000_000):
            raise ValueError('worker kills do not follow five-second schedule slots')
        previous_schedule=scheduled
        worker=fault['worker']; session=by_worker[worker]
        if worker in retired or session['pid']!=fault['pid'] or not fault.get('worker_sigkill_confirmed'):
            raise ValueError('missing confirmed SIGKILL or wrong target process')
        slot=fault['worker_slot']
        if type(slot) is not int or not 0<=slot<5 or worker!=f"matrix-process-{slot}-generation-{session['generation']}":
            raise ValueError('invalid worker slot/generation')
        if f"matrix-process-{slot}-generation-{session['generation']+1}" not in by_worker:
            raise ValueError('killed worker lacks replacement generation')
        target=fault.get('worker_target')
        if not timestamp_ns(fault['scheduled'])<=timestamp_ns(fault['killed'])<=timestamp_ns(fault['healed']):
            raise ValueError('worker kill/replacement timestamps reversed')
        if target:
            delivery=target['delivery']
            if fault.get('worker_selection')!='held_delivery' or not target['token'] or delivery['Stage']!='lease_acquired' or delivery['Worker']!=worker or not all(delivery[k] for k in ('Type','ID','RunSequence','Delivery')):
                raise ValueError('kill lacks held acquired-delivery identity')
            if not timestamp_ns(fault['scheduled'])<=timestamp_ns(delivery['At'])<=timestamp_ns(fault['killed']):
                raise ValueError('worker acquisition/kill timestamps reversed')
            steps,partial=process_lines(root/(worker+'-dispatch.jsonl'))
            if delivery not in steps:raise ValueError('held target missing from original dispatch records')
        elif fault.get('worker_selection')!='no_new_acquisition_within_500ms':
            raise ValueError('missing explicit worker selection outcome')
        retired.add(worker)
    active=sum(bool(f.get("worker_target")) for f in faults)
    if not active:raise ValueError("worker row never killed a confirmed held delivery")
    initial=set()
    for session in sessions:
        name=session['worker_id']; generation=session['generation']
        match=re.fullmatch(r'matrix-process-([0-4])-generation-(\d+)',name)
        if not match or type(generation) is not int or generation<0 or int(match[2])!=generation:
            raise ValueError('invalid session slot or generation')
        slot=int(match[1])
        if generation==0:initial.add(slot)
        elif f'matrix-process-{slot}-generation-{generation-1}' not in retired:
            raise ValueError('replacement lacks a killed predecessor')
    if initial!=set(range(5)):raise ValueError('missing initial five-process fleet')
    steps_all=[]; fences_all=[]; cross_checks=0; partial_tails=0
    for session in sessions:
        worker=session['worker_id']
        if type(session['pid']) is not int or session['pid']<=0:
            raise ValueError('invalid actual process PID')
        steps,dp=process_lines(root/(worker+'-dispatch.jsonl'))
        fences,fp=process_lines(root/(worker+'-fencing.jsonl'))
        if len(steps)!=session['dispatch_records'] or len(fences)!=session['fencing_records'] or dp!=session['partial_dispatch_tail'] or fp!=session['partial_fencing_tail']:
            raise ValueError('process counts or interrupted-tail evidence disagree')
        if any(e['Worker']!=worker for e in steps):raise ValueError('wrong dispatch process identity')
        for i,record in enumerate(fences,1):
            if record['pid']!=session['pid'] or record['sequence']!=i or record['event']['Worker']!=worker:
                raise ValueError('wrong fencing process identity or sequence')
        final=session.get('final_metrics')
        if worker in retired:
            if session.get('exit_signal')!=9 or session.get('exit_success') or final is not None or (root/(worker+'-metrics.json')).exists():raise ValueError('killed generation invents final counter cross-check')
        else:
            raw=json.loads((root/(worker+'-metrics.json')).read_text())
            if not session.get('exit_success') or session.get('exit_signal')!=0 or final is None or raw!={'pid':session['pid'],'worker_id':worker,'metrics':final} or final['fencing_events']!=len(fences) or dp or fp:
                raise ValueError('graceful process final counters disagree')
            cross_checks+=1
        partial_tails+=dp+fp
        steps_all+=steps;fences_all += [r['event'] for r in fences]
    if cross_checks!=5 or len(retired)!=len(faults):raise ValueError('missing surviving process counter checks')
    if json.loads((root/'dispatch.json').read_text())!=steps_all or (json.loads((root/'fencing.json').read_text()) or [])!=fences_all:
        raise ValueError('aggregate process evidence differs from original records')
    return dict(confirmed_sigkills=len(faults),confirmed_held_delivery_kills=active,process_generations=len(sessions),graceful_counter_cross_checks=cross_checks,
                interrupted_tails=partial_tails,killed_process_final_counters_available=False,
                complete_hard_kill_attribution=False)


def check_graceful_process_artifacts(root,sessions):
    by_worker={s['worker_id']:s for s in sessions}
    if set(by_worker)!={f'matrix-process-{i}-generation-0' for i in range(5)} or len({s['pid'] for s in sessions})!=5:
        raise ValueError('pause must keep the original five process identities')
    steps_all=[];fences_all=[]
    for session in sessions:
        worker=session['worker_id']
        if type(session['pid']) is not int or session['pid']<=0 or session['generation']!=0 or not session['exit_success'] or session['exit_signal']!=0:
            raise ValueError('pause process did not exit gracefully')
        steps,dp=process_lines(root/(worker+'-dispatch.jsonl'))
        records,fp=process_lines(root/(worker+'-fencing.jsonl'))
        raw=json.loads((root/(worker+'-metrics.json')).read_text())
        if dp or fp or session['partial_dispatch_tail'] or session['partial_fencing_tail'] or len(steps)!=session['dispatch_records'] or len(records)!=session['fencing_records']:
            raise ValueError('pause process counts or complete records disagree')
        final=session['final_metrics']
        if raw!={'pid':session['pid'],'worker_id':worker,'metrics':final} or final['fencing_events']!=len(records):
            raise ValueError('pause final counters disagree')
        if any(e['Worker']!=worker for e in steps):raise ValueError('pause dispatch worker mismatch')
        for i,r in enumerate(records,1):
            if r['pid']!=session['pid'] or r['sequence']!=i or r['event']['Worker']!=worker:raise ValueError('pause fencing PID/identity/sequence mismatch')
        steps_all+=steps;fences_all += [r['event'] for r in records]
    if json.loads((root/'dispatch.json').read_text())!=steps_all or (json.loads((root/'fencing.json').read_text()) or [])!=fences_all:
        raise ValueError('pause aggregate evidence differs from original records')
    return by_worker,steps_all,fences_all


def check_pause_artifacts(root,report):
    faults=json.loads((root/'faults.json').read_text())
    sessions=json.loads((root/'process-evidence.json').read_text())
    if len(faults)!=report['confirmed_faults'] or len(sessions)!=5:
        raise ValueError('pause count or process fleet mismatch')
    by_worker,steps_all,fences_all=check_graceful_process_artifacts(root,sessions)
    prior_schedule=None;matched=0
    for fault in faults:
        worker=fault['worker'];session=by_worker[worker];slot=fault['worker_slot']
        if fault['pid']!=session['pid'] or type(slot) is not int or worker!=f'matrix-process-{slot}-generation-0' or fault.get('worker_sigkill_confirmed'):
            raise ValueError('pause selected a different process or killed it')
        schedule,killed,paused,resumed,healed=map(timestamp_ns,[fault[k] for k in ('scheduled','killed','paused','resumed','healed')])
        if not schedule<=killed<=paused<=resumed<=healed or resumed-paused<45_000_000_000 or paused-schedule>=5_000_000_000:
            raise ValueError('pause duration or operation timeline invalid')
        if prior_schedule is not None and schedule-prior_schedule!=60_000_000_000:raise ValueError('pause cadence is not one minute')
        prior_schedule=schedule
        leases=fault['paused_leases']
        if not leases or len(leases)!=fault['active_leases'] or fault['fencing_events']<=0:
            raise ValueError('pause lacks observed active leases or resumed fencing')
        held=set()
        for lease in leases:
            observed,created=map(timestamp_ns,[lease['observed_at'],lease['created_at']])
            if lease['worker_id']!=worker or type(lease['epoch']) is not int or type(lease['revision']) is not int or lease['epoch']<=0 or lease['revision']<lease['epoch'] or not paused<=observed<=resumed or not created<=observed or resumed-created<=12_000_000_000:
                raise ValueError('paused lease snapshot identity, epoch or expiry invalid')
            held.add((lease['key'],lease['epoch']))
        if len(held)!=len(leases):raise ValueError('duplicate paused lease evidence')
        matching=[e for e in fences_all if e['Worker']==worker and resumed<=timestamp_ns(e['At'])<=healed and (e['Type']+'.'+e['ID'],e['Epoch']) in held]
        if not matching:raise ValueError('resumed fencing does not belong to a retained paused lease')
        matched+=len(matching)
    return dict(confirmed_45_second_pauses=len(faults),same_process_generations=5,graceful_counter_cross_checks=5,matched_resumed_fencing=matched,counter_cross_checks_complete=True)


def check_isolation_artifacts(root,report):
    faults=json.loads((root/'faults.json').read_text())
    sessions=json.loads((root/'process-evidence.json').read_text())
    if len(faults)!=report['confirmed_faults'] or len(sessions)!=5:raise ValueError('isolation fault/process count mismatch')
    by_worker,steps,fences=check_graceful_process_artifacts(root,sessions)
    specs=json.loads((root/'worker-proxy-specs.json').read_text())
    if len(specs)!=5 or {s['slot'] for s in specs}!=set(range(5)) or len({s['proxy_url'] for s in specs})!=5 or len({s['server_url'] for s in specs})!=5:
        raise ValueError('workers lack distinct pinned proxy/server endpoints')
    for spec in specs:
        if spec['worker_id']!=f"matrix-process-{spec['slot']}-generation-0" or spec['pid']!=by_worker[spec['worker_id']]['pid'] or spec['proxy_url']==spec['server_url']:
            raise ValueError('proxy identity or pinning mismatch')
    prior=None;matched=0
    for f in faults:
        worker=f['worker']; slot=f['worker_slot'];session=by_worker[worker]
        if f['pid']!=session['pid'] or worker!=f'matrix-process-{slot}-generation-0' or f['active_leases']<=0 or f['fencing_events']<=0:raise ValueError('isolation lacks active selected process/fencing')
        scheduled,killed,resumed,healed,ping=map(timestamp_ns,[f[k] for k in ('scheduled','killed','resumed','healed','worker_ping_at')])
        if not scheduled<=killed<=resumed<ping<=healed or resumed-killed<45_000_000_000 or killed-scheduled>=5_000_000_000 or (prior is not None and scheduled-prior!=60_000_000_000):raise ValueError('isolation cut/duration/PING/heal timeline invalid')
        prior=scheduled
        target=f['isolation_target'];d=target['delivery']
        if not target['token'] or d['Worker']!=worker or d['Stage']!='lease_acquired' or not scheduled<=timestamp_ns(d['At'])<=killed or d not in steps:raise ValueError('isolation lacks exact original acquired delivery')
        prefix=target['journal_prefix'] or []
        final=json.loads((root/f'fault-{faults.index(f)+1}-isolation-final-journal.json').read_text())
        observed=timestamp_ns(target['nonterminal_observed_at'])
        if not timestamp_ns(d['At'])<=observed<=killed or any(e['kind'] in ('Completed','Failed') for e in prefix) or (prefix and prefix[-1]['sequence']!=target['journal_tail']):raise ValueError('isolated target was already terminal or lacks cut prefix')
        if final[:len(prefix)]!=prefix or not final or final[-1]['kind']!='Completed':raise ValueError('isolated target prefix or completion invalid')
        terminal=[x for x in json.loads((root/'latencies.json').read_text()) if x['type']==d['Type'] and x['id']==d['ID'] and x['event']=='terminal']
        if len(terminal)!=1 or timestamp_ns(terminal[0]['observed'])<killed:raise ValueError('isolated target completed before the reply cut')
        before,blocked,after=[f[k] for k in ('proxy_before','proxy_blocked','proxy_healed')]
        if before['responses_held'] or not blocked['responses_held'] or after['responses_held'] or blocked['held_bytes']<=before['held_bytes'] or blocked['client_to_server']<=before['client_to_server'] or after['server_to_client']<=blocked['server_to_client'] or any(p['buffer_overflows'] for p in (before,blocked,after)):raise ValueError('reply-only traffic evidence or buffer integrity invalid')
        matching=[e for e in fences if e['Worker']==worker and e['Type']==d['Type'] and e['ID']==d['ID'] and e['RunSequence']==d['RunSequence'] and e['Delivery']==d['Delivery'] and killed<=timestamp_ns(e['At'])<=healed]
        if not matching:raise ValueError('isolation fencing does not belong to acquired delivery')
        matched+=len(matching)
    return dict(confirmed_45_second_reply_holds=len(faults),same_process_generations=5,matched_selected_delivery_fencing=matched,graceful_counter_cross_checks=5,counter_cross_checks_complete=True)


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
    if args.row == 'route_majority':
        if args.root is None: parser.error('--root is required for the majority row')
        report['majority_artifact_checks'] = check_majority_artifacts(args.root,report)
    if args.row == 'worker_kill':
        if args.root is None: parser.error('--root is required for the worker row')
        report['worker_artifact_checks'] = check_worker_artifacts(args.root,report)
    if args.row == 'worker_pause':
        if args.root is None: parser.error('--root is required for the pause row')
        report['pause_artifact_checks']=check_pause_artifacts(args.root,report)
    if args.row == 'worker_isolation':
        if args.root is None: parser.error('--root is required for isolation')
        report['isolation_artifact_checks']=check_isolation_artifacts(args.root,report)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))
