import importlib.util
from pathlib import Path
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('operator_controls', REPO/'scripts/run-operator-domain-controls.py')
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class OperatorDomainCoverageTests(unittest.TestCase):
    def test_actual_native_pair_rejects_missing_peers_routes_and_verdicts(self):
        log = (REPO/'docs/scale/operator-domain-cli-2026-10-06/native-race/native.log').read_text()
        controls.verify_log(log)
        bad = [log.replace('domain=WFOPS', 'domain=OTHER', 1),
               log.replace('node=2', 'node=1'),
               log.replace('wrong_prefix_requests=0', 'wrong_prefix_requests=1'),
               log.replace('domain_api_requests=203', 'domain_api_requests=0'),
               log.replace('--- PASS: TestOperatorCommandsInJetStreamDomain', '--- SKIP: TestOperatorCommandsInJetStreamDomain'),
               log.replace('--- PASS: TestOperatorCommands (', '--- PASS: Other ('),
               log.replace('PASS\n', '--- PASS: TestOperatorCommands (1.00s)\nPASS\n'),
               'PASS\n', log+'DATA RACE\n']
        for malformed in bad:
            self.assertNotEqual(log, malformed)
            with self.assertRaises(AssertionError):
                controls.verify_log(malformed)

    def test_original_failed_runner_cannot_qualify(self):
        log = (REPO/'docs/scale/operator-domain-cli-2026-10-06/initial-runner-failure/native.log').read_text()
        with self.assertRaises(AssertionError):
            controls.verify_log(log)


if __name__ == '__main__':
    unittest.main()
