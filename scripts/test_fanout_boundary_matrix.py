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
