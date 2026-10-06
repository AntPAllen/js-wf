import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('fanout_matrix', Path(__file__).with_name('check-fanout-boundary-matrix.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def fixture():
    events = [dict(Test=gate.TEST, Action='pass'), dict(Action='pass')]
    for case in gate.CASES:
        phase, position = case.split('/')
        cut = {'first': 0, 'interior': 250, 'last': 499}[position]
        entries = 1 + 2 * (cut + 1) if phase == 'create' else 1500 + 2 * cut
        name = gate.TEST + '/' + case
        events += [dict(Test=name, Action='pass'), dict(Test=name, Action='output', Output=(
            f'SIGKILLed parent worker at {phase} cut {cut}; retained entries={entries}\n'
            f'FANOUT_PREFIX_PRESERVED phase={phase} entries={entries} old_epoch=1 final_epoch=2\n'
            'completed 500 children; 11 shared the parent partition\n'))]
    return events


class BoundaryGateChecks(unittest.TestCase):
    def test_all_six_remain_partial_release(self):
        report = gate.check(fixture())
        self.assertEqual(len(report['cases']), 6)
        self.assertFalse(report['clears_full_release'])

    def test_missing_skipped_failed_and_ambiguous_execution(self):
        for mutation in ('missing', 'skip', 'fail', 'duplicate', 'package'):
            with self.subTest(mutation=mutation):
                events = fixture()
                if mutation == 'missing': events.pop(2)
                elif mutation == 'duplicate': events.append(copy.deepcopy(events[2]))
                elif mutation == 'package': events.pop(1)
                else: events[2]['Action'] = mutation
                with self.assertRaises(ValueError): gate.check(events)

    def test_wrong_cut_prefix_epoch_and_child_count(self):
        text = fixture()[3]['Output']
        for changed in (text.replace('cut 0;', 'cut 1;'), text.replace('entries=3 old', 'entries=4 old'),
                        text.replace('final_epoch=2', 'final_epoch=1'), text.replace('500 children', '499 children'),
                        text.replace('phase=create', 'phase=results'), text + text):
            with self.subTest(changed=changed):
                events = fixture()
                events[3]['Output'] = changed
                with self.assertRaises(ValueError): gate.check(events)


class CombinedBoundaryGateChecks(unittest.TestCase):
    def events(self):
        events = fixture()
        for event in events:
            if 'Test' in event:
                event['Test'] = event['Test'].replace(gate.TEST, gate.COMBINED_TEST)
            if 'Output' in event:
                phase = 'create' if '/create/' in event['Test'] else 'results'
                text = event['Output']
                if phase == 'results':
                    text = ('confirmed 489 outside child results before worker stop\n'
                            'confirmed 500 child results including 11 parent-partition children before worker stop\n'
                            'prepared 500 child signals before result cut without parent terminal\n') + text
                import re
                entries = int(re.search(r'retained entries=(\d+)', text).group(1))
                event['Output'] = text.replace('FANOUT_PREFIX_PRESERVED', f'combined journal restart phase={phase} node=1 messages={entries} tail_seq={entries} prefix_tail_seq={entries}\nFANOUT_PREFIX_PRESERVED')
        return events

    def test_full_combined_matrix_still_partial_release(self):
        report = gate.check(self.events(), combined=True)
        self.assertEqual(len(report['cases']), 6)
        self.assertTrue(all(c['journal_restart']['node'] == 1 for c in report['cases']))
        self.assertFalse(report['clears_full_release'])

    def test_isolated_run_does_not_qualify_combined(self):
        with self.assertRaises(ValueError): gate.check(fixture(), combined=True)

    def test_missing_wrong_phase_node_count_duplicate_or_reordered_fault(self):
        text = self.events()[3]['Output']
        restart = 'combined journal restart phase=create node=1 messages=3 tail_seq=3 prefix_tail_seq=3\n'
        for changed in (text.replace(restart, ''), text.replace('restart phase=create', 'restart phase=results'),
                        text.replace('node=1', 'node=3'), text.replace('messages=3', 'messages=2'),
                        text.replace(restart, restart + restart), restart + text.replace(restart, ''),
                        text.replace(restart, '') + restart):
            with self.subTest(changed=changed):
                events = self.events(); events[3]['Output'] = changed
                with self.assertRaises(ValueError): gate.check(events, combined=True)

    def test_archived_prefix_uses_sequence_not_live_count(self):
        events = self.events()
        event = next(e for e in events if e.get('Test', '').endswith('/results/interior') and 'Output' in e)
        original = event['Output']
        event['Output'] = original.replace('messages=2000 tail_seq=2000 prefix_tail_seq=2000', 'messages=1500 tail_seq=4194 prefix_tail_seq=4194')
        report = gate.check(events, combined=True)
        self.assertEqual(report['cases'][4]['journal_restart']['retained_journal_messages'], 1500)
        for wrong in ('messages=0 tail_seq=4194 prefix_tail_seq=4194', 'messages=1500 tail_seq=4193 prefix_tail_seq=4194', 'messages=1500 tail_seq=4194 prefix_tail_seq=1999'):
            event['Output'] = original.replace('messages=2000 tail_seq=2000 prefix_tail_seq=2000', wrong)
            with self.assertRaises(ValueError): gate.check(events, combined=True)

    def test_missing_sequence_evidence_is_rejected(self):
        events = self.events()
        events[3]['Output'] = events[3]['Output'].replace(' tail_seq=3 prefix_tail_seq=3', '')
        with self.assertRaises(ValueError): gate.check(events, combined=True)

    def test_outside_children_must_be_confirmed_before_kill(self):
        events = self.events()
        event = next(e for e in events if e.get('Test', '').endswith('/results/first') and 'Output' in e)
        original = event['Output']; marker = 'confirmed 489 outside child results before worker stop\n'
        for wrong in (original.replace(marker, ''), original.replace('confirmed 489', 'confirmed 488'), original.replace(marker, '') + marker, marker + original):
            event['Output'] = wrong
            with self.assertRaises(ValueError): gate.check(events, combined=True)

    def test_all_children_and_signal_preparation_must_precede_kill(self):
        events = self.events()
        event = next(e for e in events if e.get('Test', '').endswith('/results/last') and 'Output' in e)
        original = event['Output']
        prepared = 'prepared 500 child signals before result cut without parent terminal\n'
        all_children = 'confirmed 500 child results including 11 parent-partition children before worker stop\n'
        for wrong in (original.replace(prepared, ''), original.replace(all_children, ''), original.replace('including 11', 'including 10'), original.replace('prepared 500', 'prepared 499'), original.replace(prepared, '') + prepared, prepared + original.replace(prepared, ''), original + all_children):
            event['Output'] = wrong
            with self.assertRaises(ValueError): gate.check(events, combined=True)


class PhysicalDrainGateChecks(unittest.TestCase):
    events = CombinedBoundaryGateChecks.events
    marker = 'FANOUT_PHYSICAL_DRAIN peers=3 stream_messages=0 consumers=64 pending=0 ack_pending=0 workers_joined=64 local_monitors=3\n'

    def drained_events(self):
        events = self.events()
        for event in events:
            if 'Output' in event:
                event['Output'] = event['Output'].replace('FANOUT_PREFIX_PRESERVED', self.marker + 'FANOUT_PREFIX_PRESERVED')
        return events

    def test_complete_drain_remains_partial_release(self):
        report = gate.check(self.drained_events(), combined=True, physical_drain=True)
        self.assertTrue(report['physical_drain_required'])
        self.assertFalse(report['clears_full_release'])

    def test_missing_wrong_duplicate_and_reordered_drain(self):
        original = self.drained_events()[3]['Output']
        for changed in (original.replace(self.marker, ''), original.replace('stream_messages=0','stream_messages=1'),
                        original.replace('consumers=64','consumers=63'), original.replace(' local_monitors=3',''), original.replace('peers=3','peers=2'),
                        original.replace('ack_pending=0','ack_pending=1'), original.replace('workers_joined=64 local_monitors=3','workers_joined=63'), original.replace('workers_joined=64 local_monitors=3','workers_joined=64 local_monitors=30'),
                        original.replace(self.marker,self.marker+self.marker), self.marker+original.replace(self.marker,''),
                        original.replace(self.marker,'')+self.marker,
                        original.replace(self.marker,'').replace('combined journal restart',self.marker+'combined journal restart')):
            with self.subTest(changed=changed):
                events = self.drained_events(); events[3]['Output'] = changed
                with self.assertRaises(ValueError): gate.check(events,combined=True,physical_drain=True)
