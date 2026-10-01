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
