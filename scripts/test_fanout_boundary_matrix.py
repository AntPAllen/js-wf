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
                import re
                entries = int(re.search(r'retained entries=(\d+)', text).group(1))
                event['Output'] = text.replace('FANOUT_PREFIX_PRESERVED', f'combined journal restart phase={phase} node=1 messages={entries}\nFANOUT_PREFIX_PRESERVED')
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
        restart = 'combined journal restart phase=create node=1 messages=3\n'
        for changed in (text.replace(restart, ''), text.replace('restart phase=create', 'restart phase=results'),
                        text.replace('node=1', 'node=3'), text.replace('messages=3', 'messages=2'),
                        text.replace(restart, restart + restart), restart + text.replace(restart, ''),
                        text.replace(restart, '') + restart):
            with self.subTest(changed=changed):
                events = self.events(); events[3]['Output'] = changed
                with self.assertRaises(ValueError): gate.check(events, combined=True)
