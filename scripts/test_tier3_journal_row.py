import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('tier3_row', Path(__file__).with_name('check-tier3-journal-row.py'))
row = importlib.util.module_from_spec(spec)
spec.loader.exec_module(row)


def fixture(duration='10m'):
    seconds, faults = (600, 19) if duration == '10m' else (35, 1)
    lines = [f'TIER3_MIXED_RESULT row=journal seed=42 duration={'10m0s' if duration == '10m' else duration} five_replicas=true batches=2 invocations=56 entries=618 faults={faults} full_matrix_release=false\n']
    for typ, count in zip(row.matrix.TYPES, (8, 6, 4, 2, 12, 24)):
        lines.append(f'TIER3_MIXED_CELL type={typ} invocations={count} terminal_p99=2s progress_p99=250ms\n')
    return [dict(Test=row.TEST, Action='output', Output=''.join(lines)),
            dict(Test=row.TEST, Action='pass', Elapsed=seconds+20), dict(Action='pass')]


class JournalRowChecks(unittest.TestCase):
    def test_valid_scope_remains_partial(self):
        result = row.check(fixture(), '10m')
        self.assertEqual(result['invocations'], 56)
        self.assertFalse(result['clears_full_tier3_release'])
        alternate = fixture()
        alternate[0]['Output'] = alternate[0]['Output'].replace('duration=10m0s', 'duration=600s')
        self.assertEqual(row.check(alternate, '10m')['duration_seconds'], 600)
        self.assertTrue(row.check(fixture('35s'), '35s')['shortened_smoke'])

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
