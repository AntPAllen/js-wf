import importlib.util
from pathlib import Path
import unittest

REPO = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('controls', REPO/'scripts/run-domain-runtime-controls.py')
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class DomainCoverageControls(unittest.TestCase):
    def test_accepted_read_row_rejects_missing_duplicate_skip_and_wrong_route(self):
        log = (REPO/'docs/scale/domain-object-absence-2026-10-06/native-race/native.log').read_text()
        controls.verify_log('read-controls', log)
        malformed = [log.replace('--- PASS: TestResultReadVerifiesWeakAbsence/worker', '--- SKIP: TestResultReadVerifiesWeakAbsence/worker'),
                     log.replace('--- PASS: TestSnapshotManifestReadsUseLeaderInJetStreamDomain/object_absence', '--- PASS: OTHER/object_absence'),
                     log.replace('$JS.WFRESULT.API.STREAM.MSG.GET.OBJ_WF_BLOB', '$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB'),
                     log.replace('PASS\n', '--- PASS: TestResultReadVerifiesWeakAbsence (1.00s)\nPASS\n')]
        for bad in malformed:
            with self.subTest(bad=bad[-100:]), self.assertRaises(ValueError):
                controls.verify_log('read-controls', bad)

    def test_single_domain_kill_does_not_qualify_expiry_pair(self):
        log = (REPO/'docs/scale/continuation-domain-all-server-sigkill-2026-10-06/native-race/native.log').read_text()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            controls.verify_log('retirement-server-kill', log)

    def test_native_failure_never_qualifies(self):
        for case in controls.CASES:
            for log in ('FAIL\n', '--- SKIP: x\nPASS\n', 'DATA RACE\nPASS\n'):
                with self.subTest(case=case, log=log), self.assertRaises(ValueError):
                    controls.verify_log(case, log)
