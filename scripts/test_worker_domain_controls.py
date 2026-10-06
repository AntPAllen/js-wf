import importlib.util
from pathlib import Path
import re
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('worker_controls', REPO/'scripts/run-worker-domain-controls.py')
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class WorkerDomainCoverageTests(unittest.TestCase):
    def test_actual_native_quadruple_rejects_missing_and_wrong_coverage(self):
        log = (REPO/'docs/scale/worker-cli-domain-2026-10-06/native-race/native.log').read_text()
        controls.verify_log(log)
        stopped = re.search(r'^.*worker domain startup stopped node=0.*$', log, re.M).group()
        restarted = re.search(r'^.*worker domain startup restarted node=0.*$', log, re.M).group()
        old_id = re.search(r'server_id=(\w+)', stopped).group(1)
        new_id = re.search(r'server_id=(\w+)', restarted).group(1)
        malformed = [log.replace('domain=WFWORKER', 'domain=OTHER', 1),
                     re.sub(r'^.*worker domain admitted node=2.*\n', '', log, count=1, flags=re.M),
                     log.replace('startup stopped node=2', 'startup stopped node=1'),
                     log.replace(new_id, old_id),
                     log.replace(stopped, '__STOP__').replace(restarted, stopped).replace('__STOP__', restarted),
                     log.replace('wrong_prefix_requests=0', 'wrong_prefix_requests=1', 1),
                     re.sub(r'domain_api_requests=\d+', 'domain_api_requests=0', log, count=1),
                     re.sub(r'^.*worker real domain=.*\n', '', log, count=1, flags=re.M),
                     log.replace('--- PASS: TestWorkerRunnerStartsAfterServerRestartInJetStreamDomain', '--- SKIP: TestWorkerRunnerStartsAfterServerRestartInJetStreamDomain'),
                     log.replace('--- PASS: TestWorkerRunnerStartsAfterServerRestart (', '--- PASS: Other ('),
                     log.replace('--- PASS: TestWorkerRunnerCompletesWorkflowAndServesMetricsInJetStreamDomain/auto (', '--- PASS: Other/auto ('),
                     log+'DATA RACE\n', 'PASS\n']
        for altered in malformed:
            self.assertNotEqual(log, altered)
            with self.assertRaises(AssertionError):
                controls.verify_log(altered)

    def test_untraced_preparation_is_not_qualification(self):
        log = (REPO/'docs/scale/worker-cli-domain-2026-10-06/initial-preparation.log').read_text()
        with self.assertRaises(AssertionError):
            controls.verify_log(log)


if __name__ == '__main__':
    unittest.main()
