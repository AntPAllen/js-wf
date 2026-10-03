import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('tier3_row', Path(__file__).with_name('check-tier3-journal-row.py'))
row = importlib.util.module_from_spec(spec)
spec.loader.exec_module(row)


def fixture(duration='10m'):
    seconds = {'35s': 35, '10m': 600, '24h': 86400}[duration]
    faults = (seconds - 1) // 30
    lines = [f'TIER3_MIXED_RESULT row=journal seed=42 duration={'10m0s' if duration == '10m' else duration} five_replicas=true batches=2 invocations=56 entries=618 faults={faults} full_matrix_release=false\n']
    for typ, count in zip(row.matrix.TYPES, (8, 6, 4, 2, 12, 24)):
        lines.append(f'TIER3_MIXED_CELL type={typ} invocations={count} terminal_p99=2s progress_p99=250ms\n')
    return [dict(Test=row.TEST, Action='output', Output=''.join(lines)),
            dict(Test=row.TEST, Action='pass', Elapsed=seconds+20), dict(Action='pass')]


class JournalRowChecks(unittest.TestCase):
    def test_24_hour_rows_keep_duration_cadence_and_partial_scope(self):
        for name, faults in [('journal', 2879), ('worker_kill', 17279),
                             ('worker_pause', 1440), ('worker_isolation', 1440)]:
            events = fixture('24h')
            events[0]['Output'] = events[0]['Output'].replace('row=journal', f'row={name}').replace('faults=2879', f'faults={faults}')
            for event in events[:2]:
                event['Test'] = row.TESTS[name]
            report = row.check(events, '24h', name, 42)
            self.assertEqual(report['duration_seconds'], 86400)
            self.assertEqual(report['confirmed_faults'], faults)
            self.assertFalse(report['clears_full_tier3_release'])
            for change in ('short_elapsed', 'short_claim', 'missing_fault'):
                broken = copy.deepcopy(events)
                if change == 'short_elapsed': broken[1]['Elapsed'] = 600
                elif change == 'short_claim': broken[0]['Output'] = broken[0]['Output'].replace('duration=24h', 'duration=10m')
                else: broken[0]['Output'] = broken[0]['Output'].replace(f'faults={faults}', f'faults={faults - 1}')
                with self.subTest(row=name, change=change), self.assertRaises(ValueError):
                    row.check(broken, '24h', name, 42)

    def test_worker_clock_requires_executed_normalization_and_keeps_row_scope(self):
        for duration in ('35s','10m','24h'):
            events=fixture(duration)
            events[0]['Output']=events[0]['Output'].replace('row=journal','row=worker_clock')
            for event in events[:2]:event['Test']=row.TESTS['worker_clock']
            with self.assertRaises(ValueError):row.check(events,duration,'worker_clock',42)
            guard='TestTier3WorkerClockNormalizationPreservesRawEvidence'
            events.extend([dict(Test=guard,Action='run'),dict(Test=guard,Action='pass')])
            result=row.check(events,duration,'worker_clock',42)
            self.assertFalse(result['clears_full_tier3_release'])
            for action in ('skip','fail'):
                broken=copy.deepcopy(events);broken[-1]['Action']=action
                with self.assertRaises(ValueError):row.check(broken,duration,'worker_clock',42)

    def test_valid_scope_remains_partial(self):
        result = row.check(fixture(), '10m')
        self.assertEqual(result['invocations'], 56)
        self.assertFalse(result['clears_full_tier3_release'])
        alternate = fixture()
        alternate[0]['Output'] = alternate[0]['Output'].replace('duration=10m0s', 'duration=600s')
        self.assertEqual(row.check(alternate, '10m')['duration_seconds'], 600)
        self.assertTrue(row.check(fixture('35s'), '35s')['shortened_smoke'])

    def test_requested_seed_identity(self):
        self.assertEqual(row.check(fixture(), '10m', expected_seed=42)['seed'], 42)
        for requested in (1, 0, -1, True, '42'):
            with self.subTest(requested=requested), self.assertRaises(ValueError):
                row.check(fixture(), '10m', expected_seed=requested)

    def test_explicit_clock_duration_profiles(self):
        for profile in ('first-wait-2s', 'all-waits-2s'):
            events = fixture()
            events[0]['Test'] = row.TESTS['server_clock_behind']
            events[1]['Test'] = row.TESTS['server_clock_behind']
            events[0]['Output'] = events[0]['Output'].replace('row=journal', 'row=server_clock_behind') + f'TIER3_CLOCK_TIMER_CUT_PROFILE={profile}\n'
            self.assertEqual(row.check(events, '10m', 'server_clock_behind')['clock_timer_cut_profile'], profile)

    def test_false_green_rejected(self):
        for name in ('skip', 'fail', 'missing_package', 'wrong_duration', 'replicas', 'release',
                     'missing_workload', 'count', 'p99', 'faults', 'duplicate_result'):
            with self.subTest(name=name):
                events = copy.deepcopy(fixture())
                if name in ('skip', 'fail'): events[1]['Action'] = name
                elif name == 'missing_package': events.pop()
                elif name == 'wrong_duration': events[0]['Output'] = events[0]['Output'].replace('duration=10m0s', 'duration=35s')
                elif name == 'replicas': events[0]['Output'] = events[0]['Output'].replace('five_replicas=true', 'five_replicas=false')
                elif name == 'release': events[0]['Output'] = events[0]['Output'].replace('full_matrix_release=false', 'full_matrix_release=true')
                elif name == 'missing_workload': events[0]['Output'] = '\n'.join(line for line in events[0]['Output'].splitlines() if 'type=matrixtimer' not in line)
                elif name == 'count': events[0]['Output'] = events[0]['Output'].replace('type=matrixchild invocations=12', 'type=matrixchild invocations=11')
                elif name == 'p99': events[0]['Output'] = events[0]['Output'].replace('progress_p99=250ms', 'progress_p99=30s')
                elif name == 'faults': events[0]['Output'] = events[0]['Output'].replace('faults=19', 'faults=18')
                elif name == 'duplicate_result': events[0]['Output'] += events[0]['Output'].splitlines()[0]+'\n'
                with self.assertRaises(ValueError): row.check(events, '10m')


class CommonClockArtifactChecks(unittest.TestCase):
    def fixture(self, root):
        import json
        probes = [dict(name=f'WF_CLOCK_{n}', server=f'cluster-n{n}', identity=f'docker-node-{n}', tag=f'wf-clock-node-{n}') for n in range(5)]
        infos = [dict(config=dict(name=p['name'], subjects=['wf.clock.'+p['name']], num_replicas=1,
                                 storage='memory', retention='limits', discard='old', max_msgs=1, max_bytes=1, max_msg_size=1,
                                 placement=dict(tags=[p['tag']]), metadata=dict(workflow_clock_domain='utc-quorum-v1', workflow_clock_identity=p['identity'])),
                      cluster=dict(leader=p['server'])) for p in probes]
        proof = dict(config=dict(probes=probes, max_skewed=1, sample_budget='250ms', healthy_error='20ms', reading_age='1s', refresh='100ms'),
                     probes=infos, before='2026-10-02T04:00:00Z', lower='2026-10-02T03:59:59.95Z', upper='2026-10-02T04:00:00.05Z', after='2026-10-02T04:00:00.01Z')
        (root/'cluster').mkdir()
        for n, p in enumerate(probes):
            (root/'cluster'/f'node-{n}.conf').write_text('server_tags: '+json.dumps([p['tag']])+'\n')
        receipt = dict(entry=dict(kind='StepRequested', payload=dict(kind='timer', clock_domain='utc-quorum-v1')))
        (root/'controller-receipts.json').write_text(json.dumps([receipt]))
        return proof

    def test_requires_independent_placed_probes_and_tagged_requests(self):
        import tempfile, json
        for mutation in (None, 'profile', 'identity', 'leader', 'replicas', 'placement', 'tag', 'bounds', 'domain', 'sampling', 'count'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); proof = self.fixture(root)
                report = dict(common_timer_clock='utc-quorum-v1')
                if mutation == 'profile': report['common_timer_clock'] = None
                elif mutation == 'identity': proof['config']['probes'][4]['identity'] = 'docker-node-0'
                elif mutation == 'leader': proof['probes'][4]['cluster']['leader'] = 'cluster-n0'
                elif mutation == 'replicas': proof['probes'][4]['config']['num_replicas'] = 3
                elif mutation == 'placement': proof['probes'][4]['config']['placement']['tags'] = ['wf-clock-node-0']
                elif mutation == 'tag': (root/'cluster'/'node-4.conf').write_text('server_tags: ["wf-clock-node-0"]\n')
                elif mutation == 'bounds': proof['lower'] = '2026-10-02T04:01:00Z'
                elif mutation == 'domain': (root/'controller-receipts.json').write_text(json.dumps([dict(entry=dict(kind='StepRequested', payload=dict(kind='timer')))]))
                elif mutation == 'sampling': proof['config']['max_skewed'] = 0
                elif mutation == 'count': proof['probes'].pop()
                (root/'independent-clock.json').write_text(json.dumps(proof))
                if mutation:
                    with self.assertRaises(ValueError): row.check_common_timer_clock(root, report)
                else:
                    self.assertEqual(row.check_common_timer_clock(root, report)['canonical_timer_requests'], 1)


class ConsumerRowChecks(unittest.TestCase):
    def fixture(self):
        events = fixture()
        for event in events:
            if 'Test' in event: event['Test'] = row.TESTS['consumer']
        events[0]['Output'] = events[0]['Output'].replace('row=journal', 'row=consumer')
        events[0]['Output'] += 'TIER3_CONSUMER_FAULT consumer=WF_P_03 node=2 pending=0 ack_pending=1\n'*19
        return events

    def test_actual_named_consumer_scope_and_active_selections(self):
        result = row.check(self.fixture(), '10m', 'consumer')
        self.assertEqual(result['active_consumer_faults'], 19)
        self.assertFalse(result['clears_full_tier3_release'])

    def test_rejects_wrong_test_row_missing_idle_and_wrong_node(self):
        cases = []
        events = self.fixture(); events[1]['Test'] = row.TEST; cases.append(events)
        events = self.fixture(); events[0]['Output'] = events[0]['Output'].replace('row=consumer','row=journal'); cases.append(events)
        events = self.fixture(); events[0]['Output'] = events[0]['Output'].replace('ack_pending=1','ack_pending=0'); cases.append(events)
        events = self.fixture(); events[0]['Output'] = events[0]['Output'].replace('node=2','node=5'); cases.append(events)
        events = self.fixture(); events[0]['Output'] = events[0]['Output'].replace('TIER3_CONSUMER_FAULT','MISSING'); cases.append(events)
        for events in cases:
            with self.subTest(events=events), self.assertRaises(ValueError): row.check(events,'10m','consumer')


class ConsumerArtifactChecks(unittest.TestCase):
    def fixture(self, root):
        import json
        snapshot = dict(ObservedAt='2026-10-01T12:00:00Z',Node=2,Partition=3,Info=dict(name='WF_P_03',num_pending=0,num_ack_pending=1,cluster=dict(name='fixture',leader='fixture-n2',replicas=[{}]*4)))
        fault = dict(node=2,consumer='WF_P_03',ack_pending=1,killed='2026-10-01T12:00:01Z',healed='2026-10-01T12:00:06Z')
        (root/'faults.json').write_text(json.dumps([fault]))
        (root/'fault-1-consumer-before.json').write_text(json.dumps(snapshot))
        return snapshot, fault, dict(confirmed_faults=1,active_consumer_faults=1)

    def test_matches_actual_snapshot_identity_activity_and_heal(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);_,_,report=self.fixture(root)
            self.assertEqual(row.check_consumer_artifacts(root,report)['matched_consumer_snapshots'],1)

    def test_rejects_false_fault_identity_or_activity_even_with_passing_log(self):
        import tempfile,json
        mutations = ['leader','replicas','node','partition','name','pending','before_kill','unhealed','count']
        for mode in mutations:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);snapshot,fault,report=self.fixture(root)
                if mode=='leader':snapshot['Info']['cluster']['leader']='fixture-n1'
                elif mode=='replicas':snapshot['Info']['cluster']['replicas']=[{}]*2
                elif mode=='node':snapshot['Node']=1
                elif mode=='partition':snapshot['Partition']=64
                elif mode=='name':snapshot['Info']['name']='WF_P_04'
                elif mode=='pending':snapshot['Info']['num_ack_pending']=0
                elif mode=='before_kill':snapshot['ObservedAt']='2026-10-01T12:00:02Z'
                elif mode=='unhealed':fault['healed']='2026-10-01T11:00:00Z'
                elif mode=='count':report['confirmed_faults']=19
                (root/'faults.json').write_text(json.dumps([fault]));(root/'fault-1-consumer-before.json').write_text(json.dumps(snapshot))
                with self.assertRaises(ValueError):row.check_consumer_artifacts(root,report)


class RestartRowChecks(unittest.TestCase):
    def test_named_restart_and_scope(self):
        events = fixture('35s')
        for e in events:
            if 'Test' in e: e['Test'] = row.TESTS['restart']
        events[0]['Output'] = events[0]['Output'].replace('row=journal','row=restart')
        self.assertFalse(row.check(events,'35s','restart')['clears_full_tier3_release'])
        events[1]['Test'] = row.TEST
        with self.assertRaises(ValueError): row.check(events,'35s','restart')

    def test_all_down_boundary_and_false_controls(self):
        import tempfile,json
        actions = [('sigkill_removed',n) for n in range(5)] + [('restarted',n) for n in range(5)]
        for mode in ('valid','rolling','missing','wrong_node','reversed','no_timezone','partial_fault'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                ops=[dict(Action=a,Node=n,At=f'2026-10-01T12:00:{i+1:02d}Z') for i,(a,n) in enumerate(actions)]
                fault=dict(killed='2026-10-01T12:00:00Z',healed='2026-10-01T12:00:11Z',nodes=list(range(5)))
                if mode=='rolling':ops[1],ops[5]=ops[5],ops[1]
                elif mode=='missing':ops.pop()
                elif mode=='wrong_node':ops[4]['Node']=3
                elif mode=='reversed':ops[5]['At']='2026-10-01T11:00:00Z'
                elif mode=='no_timezone':ops[2]['At']='2026-10-01T12:00:03'
                elif mode=='partial_fault':fault['nodes']=[0,1,2]
                (root/'faults.json').write_text(json.dumps([fault]))
                (root/'fault-1-restart-operations.json').write_text(json.dumps(ops))
                if mode=='valid': self.assertEqual(row.check_restart_artifacts(root,dict(confirmed_faults=1))['confirmed_all_down_boundaries'],1)
                else:
                    with self.assertRaises(ValueError):row.check_restart_artifacts(root,dict(confirmed_faults=1))


class FanoutCutChecks(unittest.TestCase):
    def test_exact_cut_prefix_and_false_controls(self):
        import tempfile,json
        children=[f'child{i}' for i in range(6)]
        for mode in ('valid','terminal_parent','tail','duplicate','missing_pending','terminal_child','changed_prefix','late_cut','wrong_parent','missing_child','final_parent','final_children','final_grandchild'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                ops=[dict(Action=a,Node=n,At=f'2026-10-01T12:00:{i+2:02d}Z') for i,(a,n) in enumerate([('sigkill_removed',n) for n in range(5)]+[('restarted',n) for n in range(5)])]
                fault=dict(killed='2026-10-01T12:00:01Z',healed='2026-10-01T12:00:12Z',nodes=list(range(5)),fanout_parent='parent',fanout_tail=7,fanout_children=children[:],fanout_pending_children=children[:])
                prefix=[dict(kind='StepRequested',sequence=i+1,payload=dict(kind='call_async',child_id=child)) for i,child in enumerate(children)]+[dict(kind='Suspended',sequence=7)]
                cut=dict(ObservedAt='2026-10-01T12:00:00Z',Fault=copy.deepcopy(fault),ParentPrefix=prefix,ChildPrefixes={child:[dict(kind='Suspended')] for child in children})
                recovered=copy.deepcopy(prefix)
                final=dict(Parent=copy.deepcopy(prefix)+[dict(kind='Completed')],Children={},Grandchildren={})
                for child in children:
                    ids=[f'{child}-grand{i}' for i in range(2)]
                    final['Children'][child]=[dict(kind='StepRequested',payload=dict(kind='call_async',child_id=i)) for i in ids]+[dict(kind='Completed')]
                    for i in ids:final['Grandchildren'][i]=[dict(kind='Completed')]
                if mode=='terminal_parent':cut['ParentPrefix'][-1]['kind']='Completed'
                elif mode=='tail':fault['fanout_tail']=8
                elif mode=='duplicate':fault['fanout_children'][-1]=children[0]
                elif mode=='missing_pending':fault['fanout_pending_children']=[]
                elif mode=='terminal_child':cut['ChildPrefixes'][children[0]][-1]['kind']='Completed'
                elif mode=='changed_prefix':recovered[0]['sequence']=100
                elif mode=='late_cut':cut['ObservedAt']='2026-10-01T12:00:02Z'
                elif mode=='wrong_parent':cut['Fault']['fanout_parent']='other'
                elif mode=='missing_child':cut['ChildPrefixes'].pop(children[0])
                elif mode=='final_parent':final['Parent'][-1]['kind']='Suspended'
                elif mode=='final_children':final['Children'].pop(children[0])
                elif mode=='final_grandchild':next(iter(final['Grandchildren'].values()))[-1]['kind']='Failed'
                for name,value in [('faults.json',[fault]),('fault-1-restart-operations.json',ops),('fault-1-fanout-cut.json',cut),('fault-1-fanout-recovered-prefix.json',recovered),('fault-1-fanout-final.json',final)]:
                    (root/name).write_text(json.dumps(value))
                if mode=='valid':self.assertEqual(row.check_fanout_cut_artifacts(root,dict(confirmed_faults=1))['unfinished_six_child_cuts'],1)
                else:
                    with self.assertRaises(ValueError):row.check_fanout_cut_artifacts(root,dict(confirmed_faults=1))


class RouteArtifactChecks(unittest.TestCase):
    def test_raw_over_budget_is_visible_and_recovery_is_verified(self):
        import tempfile,json
        for mode in ('valid','missing_raw','false_delay','false_p99','partial_quorum','routes','unhealed','probe','count'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                stamp=lambda second:f'2026-10-01T12:00:{second:02d}Z'
                fault=dict(nodes=[0,1,2],killed=stamp(2),healed=stamp(32))
                obs=[dict(Phase='isolated',Node=n,Routes=0,At=stamp(3+n)) for n in range(3)]+[dict(Phase='reconnected',Node=n,Routes=16,At=stamp(27+n)) for n in range(5)]
                raw=[dict(type=typ,id='sample',event=event,enabled=stamp(1),observed=stamp(33),delay_ns=32_000_000_000) for typ in row.matrix.TYPES for event in ('terminal','start')]
                adjusted=[{**x,'delay_ns':1_000_000_000} for x in raw]
                report=dict(confirmed_faults=1,cells={typ:dict(invocations=1,terminal_p99_seconds=1,progress_p99_seconds=1,raw_terminal_p99_seconds=32,raw_progress_p99_seconds=32) for typ in row.matrix.TYPES})
                probe=dict(Before=1,UnacknowledgedError='context deadline exceeded')
                if mode=='missing_raw':raw.pop()
                elif mode=='false_delay':adjusted[0]['delay_ns']=0
                elif mode=='false_p99':report['cells'][row.matrix.TYPES[0]]['terminal_p99_seconds']=0
                elif mode=='partial_quorum':fault['nodes']=[0,1]
                elif mode=='routes':obs[2]['Routes']=1
                elif mode=='unhealed':fault['healed']=stamp(28)
                elif mode=='probe':probe['UnacknowledgedError']=''
                elif mode=='count':report['cells'][row.matrix.TYPES[0]]['invocations']=2
                for name,value in [('faults.json',[fault]),('fault-1-route-observations.json',obs),('fault-1-quorum-probe.json',probe),('latencies.json',raw),('route-recovery-latencies.json',adjusted)]:
                    (root/name).write_text(json.dumps(value))
                if mode=='valid':self.assertEqual(row.check_route_artifacts(root,report)['matched_recovery_samples'],12)
                else:
                    with self.assertRaises(ValueError):row.check_route_artifacts(root,report)


class MajorityArtifactChecks(unittest.TestCase):
    def test_progress_must_be_on_majority_during_cut(self):
        import tempfile,json
        for mode in ('valid','no_progress','after_heal','isolated_client','wrong_nodes','missing_routes','probe_failed','wrong_sequence'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                stamp=lambda second:f'2026-10-01T12:00:{second:02d}Z'
                fault=dict(nodes=[2],killed=stamp(1),healed=stamp(10),majority_sequence=20)
                obs=[dict(Phase='isolated',Node=2,Routes=0,At=stamp(2))]+[dict(Phase='reconnected',Node=n,Routes=16,At=stamp(5+n)) for n in range(5)]
                progress=dict(Before=10,After=20,ObservedAt=stamp(4),ClientURL='nats://majority:4222',IsolatedURL='nats://isolated:4222')
                probe=dict(Before=1,AcknowledgedSequence=2,UnacknowledgedError='')
                if mode=='no_progress':progress['After']=10
                elif mode=='after_heal':progress['ObservedAt']=stamp(11)
                elif mode=='isolated_client':progress['ClientURL']=progress['IsolatedURL']
                elif mode=='wrong_nodes':fault['nodes']=[1,2,3]
                elif mode=='missing_routes':obs[0]['Routes']=4
                elif mode=='probe_failed':probe['UnacknowledgedError']='timeout'
                elif mode=='wrong_sequence':fault['majority_sequence']=21
                for name,value in [('faults.json',[fault]),('fault-1-route-observations.json',obs),('fault-1-majority-progress.json',progress),('fault-1-quorum-probe.json',probe)]:
                    (root/name).write_text(json.dumps(value))
                if mode=='valid':self.assertEqual(row.check_majority_artifacts(root,dict(confirmed_faults=1))['confirmed_majority_progress_intervals'],1)
                else:
                    with self.assertRaises(ValueError):row.check_majority_artifacts(root,dict(confirmed_faults=1))


class WorkerKillRowChecks(unittest.TestCase):
    def fixture(self,root):
        import json
        sessions=[];steps_all=[]
        delivery=dict(At='2026-10-01T12:00:01Z',Worker='matrix-process-0-generation-0',Type='matrixshort',ID='x',Stage='lease_acquired',RunSequence=1,Delivery=1,Error='')
        for slot,generation in [(i,0) for i in range(5)]+[(0,1)]:
            worker=f'matrix-process-{slot}-generation-{generation}'
            pid=100+slot+generation*5
            steps=[delivery] if slot==generation==0 else []
            metrics=dict(fencing_events=0)
            session=dict(worker_id=worker,pid=pid,generation=generation,dispatch_records=len(steps),fencing_records=0,partial_dispatch_tail=False,partial_fencing_tail=False,exit_signal=9 if slot==generation==0 else 0,exit_success=not(slot==generation==0))
            if session['exit_success']:
                session['final_metrics']=metrics
                (root/(worker+'-metrics.json')).write_text(json.dumps(dict(pid=pid,worker_id=worker,metrics=metrics)))
            sessions.append(session);steps_all+=steps
            (root/(worker+'-dispatch.jsonl')).write_text(''.join(json.dumps(e)+'\n' for e in steps))
            (root/(worker+'-fencing.jsonl')).write_text('')
        fault=dict(worker='matrix-process-0-generation-0',worker_slot=0,pid=100,worker_sigkill_confirmed=True,worker_selection='held_delivery',scheduled='2026-10-01T12:00:00Z',killed='2026-10-01T12:00:02Z',healed='2026-10-01T12:00:03Z',worker_target=dict(token='held',delivery=delivery))
        for name,data in [('process-evidence.json',sessions),('faults.json',[fault]),('dispatch.json',steps_all),('fencing.json',[])]:
            (root/name).write_text(json.dumps(data))
        return sessions,fault,dict(confirmed_faults=1)

    def test_five_second_cadence_and_scope(self):
        events=fixture('35s')
        for e in events:
            if 'Test' in e:e['Test']=row.TESTS['worker_kill']
        events[0]['Output']=events[0]['Output'].replace('row=journal','row=worker_kill').replace('faults=1','faults=6')
        result=row.check(events,'35s','worker_kill')
        self.assertEqual(result['confirmed_faults'],6)
        self.assertFalse(result['clears_full_tier3_release'])
        events[0]['Output']=events[0]['Output'].replace('faults=6','faults=1')
        with self.assertRaises(ValueError):row.check(events,'35s','worker_kill')

    def test_process_artifacts_and_rejection_controls(self):
        import json,tempfile
        for mutation in ('none','signal','pid','target','missing_replacement','counter','false_final','aggregate','tail','graceful_failure','late_kill','all_idle'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);sessions,fault,report=self.fixture(root)
                if mutation=='signal':fault['worker_sigkill_confirmed']=False
                elif mutation=='pid':fault['pid']=999
                elif mutation=='target':fault['worker_target']['delivery']['ID']='wrong'
                elif mutation=='missing_replacement':sessions.pop()
                elif mutation=='counter':sessions[1]['final_metrics']['fencing_events']=1
                elif mutation=='false_final':sessions[0]['final_metrics']=dict(fencing_events=0)
                elif mutation=='aggregate':(root/'dispatch.json').write_text('[]')
                elif mutation=='tail':sessions[0]['partial_dispatch_tail']=True
                elif mutation=='graceful_failure':sessions[1]['exit_success']=False
                elif mutation=='late_kill':fault['killed']='2026-10-01T12:00:06Z';fault['healed']='2026-10-01T12:00:07Z'
                elif mutation=='all_idle':fault.pop('worker_target');fault['worker_selection']='no_new_acquisition_within_500ms'
                (root/'process-evidence.json').write_text(json.dumps(sessions));(root/'faults.json').write_text(json.dumps([fault]))
                if mutation=='none':
                    result=row.check_worker_artifacts(root,report)
                    self.assertEqual(result['graceful_counter_cross_checks'],5)
                    self.assertFalse(result['complete_hard_kill_attribution'])
                else:
                    with self.assertRaises((ValueError,KeyError)):row.check_worker_artifacts(root,report)

    def test_partial_killed_tail_retained_without_counting(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);sessions,_,report=self.fixture(root)
            path=root/'matrix-process-0-generation-0-fencing.jsonl'
            path.write_text('{"pid":')
            sessions[0]['partial_fencing_tail']=True
            (root/'process-evidence.json').write_text(json.dumps(sessions))
            result=row.check_worker_artifacts(root,report)
            self.assertEqual(result['interrupted_tails'],1)
            self.assertEqual(path.read_text(),'{"pid":')

class WorkerPauseRowChecks(unittest.TestCase):
    def fixture(self,root):
        import json
        sessions=[];steps_all=[];fences_all=[]
        for slot in range(5):
            worker=f'matrix-process-{slot}-generation-0';pid=200+slot
            event=dict(At='2026-10-01T12:00:47Z',Worker=worker,Type='matrixshort',ID='held',RunSequence=1,Delivery=1,Epoch=7,Reason='lease_heartbeat_lost',Error='lease was lost')
            records=[dict(pid=pid,sequence=1,event=event)] if slot==0 else []
            metrics=dict(fencing_events=len(records))
            sessions.append(dict(worker_id=worker,pid=pid,generation=0,dispatch_records=0,fencing_records=len(records),partial_dispatch_tail=False,partial_fencing_tail=False,exit_signal=0,exit_success=True,final_metrics=metrics))
            (root/(worker+'-dispatch.jsonl')).write_text('')
            (root/(worker+'-fencing.jsonl')).write_text(''.join(json.dumps(r)+'\n' for r in records))
            (root/(worker+'-metrics.json')).write_text(json.dumps(dict(pid=pid,worker_id=worker,metrics=metrics)))
            fences_all += [r['event'] for r in records]
        fault=dict(worker='matrix-process-0-generation-0',worker_slot=0,pid=200,scheduled='2026-10-01T12:00:00Z',killed='2026-10-01T12:00:01Z',paused='2026-10-01T12:00:01Z',resumed='2026-10-01T12:00:46Z',healed='2026-10-01T12:00:48Z',active_leases=1,fencing_events=1,paused_leases=[dict(key='matrixshort.held',worker_id='matrix-process-0-generation-0',epoch=7,revision=8,created_at='2026-10-01T11:59:59Z',observed_at='2026-10-01T12:00:02Z')])
        for name,data in [('process-evidence.json',sessions),('faults.json',[fault]),('dispatch.json',steps_all),('fencing.json',fences_all)]: (root/name).write_text(json.dumps(data))
        return sessions,fault,dict(confirmed_faults=1)

    def test_pause_scope_and_fault_cadence(self):
        for duration,count in [('35s',1),('10m',10)]:
            events=fixture(duration)
            for event in events:
                if 'Test' in event:event['Test']=row.TESTS['worker_pause']
            events[0]['Output']=events[0]['Output'].replace('row=journal','row=worker_pause').replace('faults=19',f'faults={count}')
            result=row.check(events,duration,'worker_pause')
            self.assertEqual(result['confirmed_faults'],count)
            self.assertFalse(result['clears_full_tier3_release'])

    def test_pause_exact_lease_fencing_and_false_green_controls(self):
        import tempfile,json
        for mutation in ('none','short','pid','epoch','wrong_key','no_held','no_fencing','interrupted','killed','replacement','counter','clock'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);sessions,fault,report=self.fixture(root)
                if mutation=='short':fault['resumed']='2026-10-01T12:00:45Z'
                elif mutation=='pid':fault['pid']=999
                elif mutation=='epoch':fault['paused_leases'][0]['epoch']=6
                elif mutation=='wrong_key':fault['paused_leases'][0]['key']='matrixshort.other'
                elif mutation=='no_held':fault['paused_leases']=[]
                elif mutation=='no_fencing':fault['fencing_events']=0
                elif mutation=='interrupted':sessions[0]['partial_fencing_tail']=True
                elif mutation=='killed':sessions[0]['exit_signal']=9
                elif mutation=='replacement':sessions[0]['worker_id']='matrix-process-0-generation-1'
                elif mutation=='counter':sessions[0]['final_metrics']['fencing_events']=0
                elif mutation=='clock':fault['paused_leases'][0]['observed_at']='2026-10-01T12:00:49Z'
                (root/'process-evidence.json').write_text(json.dumps(sessions));(root/'faults.json').write_text(json.dumps([fault]))
                if mutation=='none':
                    result=row.check_pause_artifacts(root,report)
                    self.assertEqual(result['matched_resumed_fencing'],1)
                    self.assertTrue(result['counter_cross_checks_complete'])
                else:
                    with self.assertRaises((ValueError,KeyError)):row.check_pause_artifacts(root,report)

class WorkerIsolationRowChecks(unittest.TestCase):
    def fixture(self,root):
        import json
        sessions,fault,report=WorkerPauseRowChecks().fixture(root)
        worker=fault['worker']
        fetch=dict(At='2026-10-01T12:00:00.4Z',Worker=worker,Type='matrixshort',ID='held',RunSequence=1,Delivery=1,Stage='fetched',Error='')
        delivery=dict(fetch,At='2026-10-01T12:00:00.5Z',Stage='lease_acquired')
        fault.update(isolation_target=dict(token='acquired',delivery=delivery,journal_prefix=[],journal_tail=0,nonterminal_observed_at='2026-10-01T12:00:00.6Z'),worker_ping_at='2026-10-01T12:00:47.5Z')
        (root/'fault-1-isolation-final-journal.json').write_text(json.dumps([dict(sequence=2,kind='Completed')]))
        (root/'latencies.json').write_text(json.dumps([dict(type='matrixshort',id='held',event='terminal',observed='2026-10-01T12:00:20Z')]))
        stats=dict(client_to_server=10,server_to_client=20,held_bytes=0,responses_held=False,buffer_overflows=0)
        fault.update(proxy_before=stats,proxy_blocked=dict(stats,client_to_server=11,held_bytes=1,responses_held=True),proxy_healed=dict(stats,client_to_server=12,server_to_client=21,held_bytes=1))
        (root/(worker+'-dispatch.jsonl')).write_text(json.dumps(fetch)+'\n'+json.dumps(delivery)+'\n')
        (root/'dispatch.json').write_text(json.dumps([fetch,delivery]))
        sessions[0]['dispatch_records']=2
        specs=[dict(worker_id=s['worker_id'],pid=s['pid'],slot=i,proxy_url=f'nats://127.0.0.1:{100+i}',server_url=f'nats://127.0.0.1:{200+i}') for i,s in enumerate(sessions)]
        (root/'worker-proxy-specs.json').write_text(json.dumps(specs))
        (root/'process-evidence.json').write_text(json.dumps(sessions));(root/'faults.json').write_text(json.dumps([fault]))
        return sessions,fault,specs,report

    def test_reply_isolation_scope_and_count(self):
        for duration,count in [('35s',1),('10m',10)]:
            events=fixture(duration)
            for e in events:
                if 'Test' in e:e['Test']=row.TESTS['worker_isolation']
            events[0]['Output']=events[0]['Output'].replace('row=journal','row=worker_isolation').replace('faults=19',f'faults={count}')
            result=row.check(events,duration,'worker_isolation')
            self.assertEqual(result['confirmed_faults'],count)
            self.assertFalse(result['clears_full_tier3_release'])

    def test_asymmetry_exact_delivery_health_and_false_green_controls(self):
        import json,tempfile
        for mutation in ('none','short','stale_ping','requests_blocked','replies_not_held','no_held_bytes','overflow','no_healed_replies','wrong_delivery','wrong_pid','bypass_proxy','duplicate_proxy','terminal_before_cut','terminal_prefix','changed_prefix'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);sessions,fault,specs,report=self.fixture(root)
                if mutation=='short':fault['resumed']='2026-10-01T12:00:45Z'
                elif mutation=='stale_ping':fault['worker_ping_at']='2026-10-01T12:00:45Z'
                elif mutation=='requests_blocked':fault['proxy_blocked']['client_to_server']=10
                elif mutation=='replies_not_held':fault['proxy_blocked']['responses_held']=False
                elif mutation=='no_held_bytes':fault['proxy_blocked']['held_bytes']=0
                elif mutation=='overflow':fault['proxy_healed']['buffer_overflows']=1
                elif mutation=='no_healed_replies':fault['proxy_healed']['server_to_client']=20
                elif mutation=='wrong_delivery':fault['isolation_target']['delivery']['RunSequence']=2
                elif mutation=='wrong_pid':fault['pid']=999
                elif mutation=='bypass_proxy':specs[0]['proxy_url']=specs[0]['server_url']
                elif mutation=='duplicate_proxy':specs[0]['proxy_url']=specs[1]['proxy_url']
                elif mutation=='terminal_before_cut':(root/'latencies.json').write_text(json.dumps([dict(type='matrixshort',id='held',event='terminal',observed='2026-10-01T12:00:00.7Z')]))
                elif mutation=='terminal_prefix':fault['isolation_target']['journal_prefix']=[dict(sequence=1,kind='Completed')];fault['isolation_target']['journal_tail']=1
                elif mutation=='changed_prefix':fault['isolation_target']['journal_prefix']=[dict(sequence=1,kind='Started')];fault['isolation_target']['journal_tail']=1
                (root/'faults.json').write_text(json.dumps([fault]));(root/'worker-proxy-specs.json').write_text(json.dumps(specs))
                if mutation=='none':
                    result=row.check_isolation_artifacts(root,report)
                    self.assertEqual(result['matched_selected_delivery_fencing'],1)
                    self.assertTrue(result['counter_cross_checks_complete'])
                else:
                    with self.assertRaises((ValueError,KeyError)):row.check_isolation_artifacts(root,report)


class ServerClockRowChecks(unittest.TestCase):
    def test_named_clock_rows_keep_raw_gate_and_partial_scope(self):
        for selected in ('server_clock_ahead', 'server_clock_behind'):
            events = fixture('35s')
            for event in events:
                if 'Test' in event:
                    event['Test'] = row.TESTS[selected]
            events[0]['Output'] = events[0]['Output'].replace('row=journal', f'row={selected}')
            result = row.check(events, '35s', selected)
            self.assertFalse(result['clears_full_tier3_release'])
            self.assertEqual(result['confirmed_faults'], 1)
            events[0]['Output'] = events[0]['Output'].replace('terminal_p99=2s', 'terminal_p99=30s')
            with self.assertRaises(ValueError):
                row.check(events, '35s', selected)

    def test_actual_clock_and_journal_kill_boundary_controls(self):
        from datetime import datetime, timedelta, timezone
        import json
        import tempfile
        origin = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        stamp = lambda seconds: (origin + timedelta(seconds=seconds)).isoformat()
        for selected, offset in (('server_clock_ahead', 60), ('server_clock_behind', -60)):
            for mode in ('valid', 'missing_clock', 'duplicate', 'unshifted', 'other_node_shifted',
                         'wrong_expected', 'slow_read', 'reversed_read', 'late_before', 'early_after',
                         'missing_fault', 'wrong_leader', 'wrong_replicas', 'wrong_stream',
                         'missing_removal', 'wrong_process', 'reversed_restart'):
                with self.subTest(row=selected, mode=mode), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    fault = dict(node=2, scheduled=stamp(30), killed=stamp(31), healed=stamp(34))
                    observations = []
                    for stage, base in (('initial', 1), ('before', 30.1), ('after', 34.1)):
                        for node in range(5):
                            before = base + node / 100
                            want = offset if node == 4 else 0
                            observations.append(dict(stage=stage, fault=0 if stage == 'initial' else 1, node=node,
                                                     host_before=stamp(before), host_after=stamp(before+.002),
                                                     server_now=stamp(before+.001+want), expected_offset_ns=want*1_000_000_000))
                    info = dict(config=dict(name='WF_JRN', num_replicas=5), cluster=dict(leader='fixture-n2', replicas=[{}]*4))
                    operations = [dict(node=2, action='sigkill_removed', at=stamp(32)), dict(node=2, action='restarted', at=stamp(33))]
                    if mode == 'missing_clock': observations.pop()
                    elif mode == 'duplicate': observations.append(observations[0])
                    elif mode == 'unshifted': observations[4]['server_now'] = observations[4]['host_before']
                    elif mode == 'other_node_shifted': observations[0]['server_now'] = stamp(61)
                    elif mode == 'wrong_expected': observations[4]['expected_offset_ns'] = 0
                    elif mode == 'slow_read': observations[0]['host_after'] = stamp(4)
                    elif mode == 'reversed_read': observations[0]['host_after'] = stamp(0)
                    elif mode == 'late_before':
                        observations[5]['host_before'] = stamp(32)
                        observations[5]['host_after'] = stamp(32.002)
                        observations[5]['server_now'] = stamp(32.001)
                    elif mode == 'early_after':
                        observations[10]['host_before'] = stamp(33)
                        observations[10]['host_after'] = stamp(33.002)
                        observations[10]['server_now'] = stamp(33.001)
                    elif mode == 'wrong_leader': info['cluster']['leader'] = 'fixture-n3'
                    elif mode == 'wrong_replicas': info['config']['num_replicas'] = 3
                    elif mode == 'wrong_stream': info['config']['name'] = 'WF_RUN'
                    elif mode == 'missing_removal': operations.pop(0)
                    elif mode == 'wrong_process': operations[0]['node'] = 3
                    elif mode == 'reversed_restart': operations[1]['at'] = stamp(31)
                    (root/'faults.json').write_text(json.dumps([] if mode == 'missing_fault' else [fault]))
                    (root/'server-clock-observations.json').write_text(json.dumps(observations))
                    (root/'fault-1-journal-before.json').write_text(json.dumps(info))
                    (root/'fault-1-journal-operations.json').write_text(json.dumps(operations))
                    if mode == 'valid':
                        result = row.check_server_clock_artifacts(root, dict(confirmed_faults=1), selected)
                        self.assertEqual(result['actual_clock_observations'], 15)
                        self.assertEqual(result['expected_offset_ns'], offset*1_000_000_000)
                    else:
                        with self.assertRaises(ValueError):
                            row.check_server_clock_artifacts(root, dict(confirmed_faults=1), selected)


class ClockRoleAdmissionChecks(unittest.TestCase):
    def test_role_changes_and_shifted_lookup_are_required(self):
        import json,tempfile
        stamp=lambda second:f'2026-10-01T12:00:{second:02d}Z'
        for selected,server in [('server_clock_ahead','2026-10-01T12:01:01Z'),('server_clock_behind','2026-10-01T11:59:01Z')]:
            for mode in ('valid','native_hint','duplicate_hint','unknown_hint','idle_peer','same_clock','wrong_kill','missing_role','duplicate_role','no_skew_lookup','wrong_boundary'):
                with self.subTest(row=selected,mode=mode),tempfile.TemporaryDirectory() as directory:
                    root=Path(directory)
                    faults=[dict(node=4,scheduled=stamp(30),killed=stamp(31),healed=stamp(36))]
                    operations=[dict(at=stamp(32)),dict(at=stamp(35))]
                    roles=[]
                    for stage,fault,at,node in [('initial',0,1,4),('before',1,30,4),('replacement',1,33,1),('after',1,35,1)]:
                        for name in ('WF_RUN','WF_JRN'):
                            roles.append(dict(stage=stage,fault=fault,stream=name,observed=stamp(at),info=dict(config=dict(name=name,num_replicas=5),cluster=dict(leader=f'fixture-n{node}',replicas=[{}]*4))))
                    clocks=[dict(Operation='timer_clock',At=stamp(1),Duration=1_000_000,ServerTime=server)]
                    if mode in ('native_hint','duplicate_hint','unknown_hint'):
                        clocks[0].update(Operation='timer_native_hint',ClockDomain='utc-quorum-v1',TimerPublished=mode!='duplicate_hint')
                        if mode=='unknown_hint':clocks[0]['Error']='outcome unknown'
                    if mode=='idle_peer':roles[0]['info']['cluster']['leader']='fixture-n0'
                    elif mode=='same_clock':roles[4]['info']['cluster']['leader']='fixture-n4'
                    elif mode=='wrong_kill':faults[0]['node']=1
                    elif mode=='missing_role':roles.pop()
                    elif mode=='duplicate_role':roles.append(copy.deepcopy(roles[0]))
                    elif mode=='no_skew_lookup':clocks[0]['ServerTime']=stamp(1)
                    elif mode=='wrong_boundary':roles[4]['observed']=stamp(31)
                    for name,data in [('faults.json',faults),('server-clock-roles.json',roles),('fault-1-journal-operations.json',operations),('controller-operations.json',clocks)]:
                        (root/name).write_text(json.dumps(data))
                    if mode in ('valid','native_hint'):
                        result=row.check_server_clock_role_artifacts(root,dict(confirmed_faults=1),selected)
                        self.assertEqual(result['role_observations'],8)
                        self.assertFalse(result['admits_all_in_flight_timer_cut_combinations'])
                    else:
                        with self.assertRaises(ValueError):row.check_server_clock_role_artifacts(root,dict(confirmed_faults=1),selected)
