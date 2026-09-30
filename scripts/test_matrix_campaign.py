import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('matrix_gate', Path(__file__).with_name('check-matrix-campaign.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
TEST = 'TestMixedMatrixRandomWorkerKilledEveryFiveSeconds'


def log(seed):
    lines = ['worker faults=119 active_worker_faults=20 row=worker_kill',
             f'MATRIX_RESULT row=worker_kill seed={seed} duration=10m0s release_duration=true batches=1 invocations=28 faults=119 terminal_p99=15s',
             'MATRIX_RETAINED row=worker_kill report={Invocations:28 Journals:28 Entries:300 Terminal:28} expected_invocations=28',
             f'--- PASS: {TEST} (628.65s)']
    for typ, count in zip(gate.TYPES, (4, 3, 2, 1, 6, 12)):
        lines.extend([f'MATRIX_CELL type={typ} invocations={count} terminal_p99=28.01s',
                      f'MATRIX_PROGRESS type={typ} enabled_events=12 progress_p99=473.5ms progress_max=13s above_30s=0'])
    return '\n'.join(lines)


class CampaignControls(unittest.TestCase):
    def setUp(self):
        self.metadata = {'headSha': 'a' * 40, 'status': 'completed', 'conclusion': 'success',
                         'jobs': [{'name': name, 'status': 'completed', 'conclusion': 'success'}
                                  for name in ['seeds'] + [f'leader ({i})' for i in range(1, 21)]]}
        self.logs = {i: log(i) for i in range(1, 21)}

    def check(self):
        return gate.check_campaign(self.metadata, self.logs, 'worker_kill', TEST, 20)

    def test_complete_row(self):
        report = self.check()
        self.assertEqual(report['invocations'], 560)
        self.assertEqual(report['faults'], 2380)
        self.assertEqual(report['scope'], 'one fault row at the recorded revision')

    def test_missing_duplicate_or_failed_jobs(self):
        original = copy.deepcopy(self.metadata)
        for mutation in ('missing', 'duplicate', 'failed', 'live'):
            self.metadata = copy.deepcopy(original)
            if mutation == 'missing':
                self.metadata['jobs'].pop()
            elif mutation == 'duplicate':
                self.metadata['jobs'][-1] = self.metadata['jobs'][1]
            elif mutation == 'failed':
                self.metadata['jobs'][-1]['conclusion'] = 'failure'
            else:
                self.metadata['status'] = 'in_progress'
            with self.assertRaises(ValueError):
                self.check()

    def test_smoke_wrong_seed_and_missing_pass(self):
        original = self.logs[20]
        for replacement in (original.replace('10m0s', '35s'), original.replace('release_duration=true', 'release_duration=false'),
                            original.replace('seed=20', 'seed=1'), original.replace('--- PASS:', '--- FAIL:')):
            self.logs[20] = replacement
            with self.assertRaises(ValueError):
                self.check()

    def test_aggregate_cell_and_progress_gate_boundaries(self):
        original = self.logs[20]
        for old in ('terminal_p99=15s', 'terminal_p99=28.01s', 'progress_p99=473.5ms'):
            self.logs[20] = original.replace(old, old.split('=')[0] + '=30s', 1)
            with self.assertRaises(ValueError):
                self.check()

    def test_counts_missing_and_duplicate_markers(self):
        original = self.logs[20]
        for replacement in (original.replace('Journals:28', 'Journals:27'),
                            original.replace('active_worker_faults=20', 'active_worker_faults=0'),
                            original.replace('matrixshort invocations=4', 'matrixshort invocations=3'),
                            original.replace('MATRIX_PROGRESS type=matrixsignal', 'MISSING type=matrixsignal'),
                            original + '\n' + original.splitlines()[0]):
            self.logs[20] = replacement
            with self.assertRaises(ValueError):
                self.check()
        del self.logs[20]
        with self.assertRaises(ValueError):
            self.check()

    def test_duration_parser_rejects_partial_and_negative_values(self):
        self.assertAlmostEqual(gate.seconds('1m2.3s'), 62.3)
        for value in ('junk15s', '-1s', 'NaNs', '30sxxx', ''):
            with self.assertRaises(ValueError):
                gate.seconds(value)


if __name__ == '__main__':
    unittest.main()
