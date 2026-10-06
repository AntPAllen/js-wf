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


class OperatorDaemonCoverageTests(unittest.TestCase):
    def test_actual_daemon_pair_rejects_wrong_signals_peers_cases_and_fatal_masking(self):
        log=(REPO/'docs/scale/operator-daemon-signals-2026-10-06/native-race/native.log').read_text()
        controls.verify_daemon_log(log)
        bad=[log.replace('domain=WFOPS','domain=OTHER',1),
             log.replace('domain admitted node=2','domain admitted node=1'),
             log.replace('stage=startup domain="" signal=terminated','stage=startup domain="" signal=interrupt'),
             log.replace('stage=running domain="" signal=interrupt','stage=running domain="" signal=terminated'),
             log.replace('error=stream-not-found exit=1','error=stream-not-found exit=0'),
             log.replace('--- PASS: TestOperatorDaemonSignals/project/startup','--- PASS: Other/project/startup'),
             log.replace('--- PASS: TestOperatorDaemonSignalsInJetStreamDomain','--- SKIP: TestOperatorDaemonSignalsInJetStreamDomain',1),
             log+'DATA RACE\n','PASS\n']
        for altered in bad:
            self.assertNotEqual(log,altered)
            with self.assertRaises(AssertionError):controls.verify_daemon_log(altered)

    def test_original_readiness_failure_cannot_qualify(self):
        log=(REPO/'docs/scale/operator-daemon-signals-2026-10-06/initial-native-failure/native.log').read_text()
        with self.assertRaises(AssertionError):controls.verify_daemon_log(log)


if __name__ == '__main__':
    unittest.main()
