import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('clock_row', Path(__file__).with_name('check-tier3-journal-row.py'))
row = importlib.util.module_from_spec(spec)
spec.loader.exec_module(row)
controller = row.load('controller_contract', 'check-controller-latency.py')
FIXTURE = Path(__file__).parents[1]/'docs/scale/controller-latency-audit-2026-10-01'


class ControllerLatencyChecks(unittest.TestCase):
    def test_retained_native_eight_timer_proof(self):
        report = json.loads((FIXTURE/'contract-report.json').read_text())
        result = controller.check(FIXTURE, report, row.timestamp_ns)
        self.assertEqual(result['append_windows'], 26)
        self.assertEqual(result['timer_waits'], 8)
        self.assertFalse(result['uses_broker_timestamps'])

    def test_corrupt_or_incomplete_observations_fail(self):
        for mode in ('missing_call', 'unknown_without_receipt', 'wrong_receipt', 'duplicate_receipt',
                     'wrong_bound', 'wrong_clock', 'missing_sample', 'wrong_delay', 'wrong_p99',
                     'early_timer', 'missing_timer', 'duplicate_window', 'missing_clock_lookup', 'wrong_clock_lookup', 'missing_client_call', 'late_start_anchor', 'cut_profile_without_long_wait', 'unknown_profile'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                files = {p.name: json.loads(p.read_text()) for p in FIXTURE.glob('*.json')}
                report = files['contract-report.json']
                proof = files['controller-latency-audit.json']
                operations = files['controller-operations.json']
                receipts = files['controller-receipts.json']
                if mode == 'cut_profile_without_long_wait': report['clock_timer_cut_profile'] = 'first-wait-2s'
                elif mode == 'unknown_profile': report['clock_timer_cut_profile'] = 'anything'
                elif mode == 'missing_call': operations.pop(0)
                elif mode == 'unknown_without_receipt':
                    for e in operations: e['Error'] = 'journal append outcome unknown'
                    receipts.clear()
                elif mode == 'wrong_receipt': receipts[0]['entry']['epoch'] += 1
                elif mode == 'duplicate_receipt': receipts.append(copy.deepcopy(receipts[0]))
                elif mode == 'wrong_bound': proof['bounds'][0]['before'] = '2026-10-01T00:00:00Z'
                elif mode == 'wrong_clock': proof['clock'] = 'broker'
                elif mode == 'missing_sample': proof['samples'].pop()
                elif mode == 'wrong_delay': proof['samples'][0]['delay_ns'] += 1
                elif mode == 'wrong_p99': report['cells']['matrixtimer']['terminal_p99_seconds'] += 1
                elif mode == 'early_timer': files['controller-timers.json'][0]['first_return'] = files['controller-timers.json'][0]['first_call']
                elif mode == 'missing_timer': files['controller-timers.json'].pop()
                elif mode == 'duplicate_window': proof['bounds'].append(copy.deepcopy(proof['bounds'][0]))
                elif mode == 'missing_clock_lookup': operations[:] = [e for e in operations if e['Operation'] != 'timer_clock']
                elif mode == 'wrong_clock_lookup':
                    next(e for e in operations if e['Operation'] == 'timer_clock')['ServerTime'] = '1970-01-01T00:00:00Z'
                elif mode == 'missing_client_call': files['controller-client-calls.json'][:] = [e for e in files['controller-client-calls.json'] if e['op'] != 'start']
                elif mode == 'late_start_anchor':
                    start = next(s for s in proof['samples'] if s['event'] == 'start')
                    start['enabled'] = start['observed_lower']
                    start['delay_ns'] = row.timestamp_ns(start['observed']) - row.timestamp_ns(start['enabled'])
                # Keep the sample copy consistent so mutation controls reach the
                # derived-window checks rather than only a duplicate-file check.
                files['latencies.json'] = copy.deepcopy(proof['samples'])
                for name, value in files.items(): (root/name).write_text(json.dumps(value))
                with self.assertRaises(ValueError): controller.check(root, report, row.timestamp_ns)
