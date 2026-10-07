import importlib.util
from pathlib import Path
import re
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('worker_controls', REPO/'scripts/run-worker-domain-controls.py')
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class WorkerDomainCoverageTests(unittest.TestCase):
    def test_actual_leaf_log_requires_all_modes_and_child_wire_captures(self):
        log=(REPO/'docs/scale/worker-leaf-wire-2026-10-07/native-race/native.log').read_text()
        controls.verify_leaf_log(log)
        for token in ('worker leaf wire: test=TestWorkerStandaloneCommandsThroughLeaf/static',
                      'worker standalone process domain="WFWORKER"'):
            line=next(line for line in log.splitlines() if token in line)
            for malformed in (log.replace(line,''),log+'\n'+line):
                with self.assertRaises(AssertionError):controls.verify_leaf_log(malformed)
        for malformed in (log.replace('truncated=false','truncated=true',1),
                          log.replace('local=WFEDGE','local=WFWORKER',1),
                          log.replace('--- PASS: TestWorkerStandaloneCommandsThroughLeaf/auto','--- SKIP: TestWorkerStandaloneCommandsThroughLeaf/auto')):
            with self.assertRaises(AssertionError):controls.verify_leaf_log(malformed)

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

    def test_actual_standalone_log_rejects_missing_or_altered_process_contracts(self):
        log=(REPO/'docs/scale/worker-standalone-cli-2026-10-06/native-race/native.log').read_text()
        controls.verify_standalone_log(log)
        process=re.search(r'^.*worker standalone process .*$',log,re.M).group()
        old=re.search(r'worker domain startup stopped node=0 domain=WFWORKER server_id=(\w+)',log).group(1)
        new=re.search(r'worker domain startup restarted node=0 domain=WFWORKER server_id=(\w+)',log).group(1)
        variants=[log.replace(process,'',1),log.replace('signal=SIGTERM','signal=SIGKILL',1),log.replace('exit=0','exit=1',1),log.replace(process,process+'\n'+process,1),log.replace('domain="WFWORKER"','domain="OTHER"',1),log.replace(new,old),log.replace('--- PASS: TestWorkerStandaloneCommands/static (','--- PASS: Other/static ('),log.replace('--- PASS: TestWorkerStandaloneStartsAfterServerRestart (','--- SKIP: TestWorkerStandaloneStartsAfterServerRestart ('),log+'DATA RACE\n']
        for altered in variants:
            self.assertNotEqual(log,altered)
            with self.assertRaises(AssertionError):controls.verify_standalone_log(altered)

    def test_untraced_preparation_is_not_qualification(self):
        log = (REPO/'docs/scale/worker-cli-domain-2026-10-06/initial-preparation.log').read_text()
        with self.assertRaises(AssertionError):
            controls.verify_log(log)


if __name__ == '__main__':
    unittest.main()
