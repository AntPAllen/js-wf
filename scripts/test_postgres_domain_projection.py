import copy
import importlib.util
import json
from pathlib import Path
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('domain_projection', REPO/'scripts/review-postgres-domain-projection.py')
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)
NATIVE = REPO/'docs/scale/postgres-domain-projection-50000-2026-10-06/native'


class PostgresDomainProjectionCoverage(unittest.TestCase):
    def test_actual_full_fault_proof_rejects_missing_or_wrong_domain_faults(self):
        proof = json.loads((NATIVE/'projection-fault-proof.json').read_text())
        log = (NATIVE/'native.log').read_text()
        killed = (NATIVE/'killed-projection.log').read_text()
        sdk = json.loads((NATIVE/'binary.json').read_text())['sha256']
        accepted = review.verify_fault(proof, log, killed, sdk)
        self.assertEqual(accepted['count'], 50000)
        for key, value in [('count', 25000), ('native_test_failed', True),
                           ('projection_domain', ''), ('actual_projection_sdk_domain', 'OTHER'),
                           ('actual_projection_sdk_sha256', '0'*64), ('projection_process_reaped_sigkill', False),
                           ('domain_api_requests', 0), ('wrong_domain_api_prefix_requests', 1),
                           ('stopped_projection_lag', 99999), ('all_results_checked_while_projection_stopped', False),
                           ('catchup_admitted_rows', 50000), ('writer_backend_termination_confirmed', False),
                           ('final_lag', 1), ('journal_leader_new_server_id', proof['journal_leader_old_server_id']),
                           ('healed_domain_peers', proof['healed_domain_peers'][:2]),
                           ('fault_started', proof['projection_process_observed_before_kill'])]:
            altered = copy.deepcopy(proof)
            altered[key] = value
            with self.subTest(field=key), self.assertRaises(ValueError):
                review.verify_fault(altered, log, killed, sdk)

    def test_actual_native_and_helper_logs_cannot_be_replaced_by_verdicts(self):
        proof = json.loads((NATIVE/'projection-fault-proof.json').read_text())
        log = (NATIVE/'native.log').read_text()
        killed = (NATIVE/'killed-projection.log').read_text()
        sdk = json.loads((NATIVE/'binary.json').read_text())['sha256']
        for altered, helper in [('PASS\n', killed),
                                (log.replace('--- PASS:', '--- SKIP:', 1), killed),
                                (log.replace('wrong_prefix_requests=0', 'wrong_prefix_requests=1'), killed),
                                (log, killed.replace('domain=WFVIEW', 'domain=OTHER'))]:
            with self.assertRaises(ValueError):
                review.verify_fault(proof, altered, helper, sdk)


if __name__ == '__main__':
    unittest.main()
