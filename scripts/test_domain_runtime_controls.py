import importlib.util
from pathlib import Path
import unittest
import re

REPO = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('controls', REPO/'scripts/run-domain-runtime-controls.py')
controls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controls)


class DomainCoverageControls(unittest.TestCase):
    def test_actual_legacy_row_requires_all_peer_versions_and_fallback_after_restart(self):
        log = (REPO/'docs/scale/domain-retirement-legacy-weak-frame-expiry-2026-10-06/native-race/native.log').read_text()
        controls.verify_log('legacy-retirement-weak-frame-expiry', log)
        bad = [log.replace('version=2.11.17', 'version=2.15.0', 1),
               log.replace('timer_backend=fallback', 'timer_backend=native', 1),
               log.replace('legacy domain admitted node=0', 'legacy domain admitted node=1'),
               log.replace('legacy domain healed node=2', 'missing domain healed node=2'),
               re.sub(r'prior_epoch=(\d+) terminal_epoch=\d+', lambda m: f'prior_epoch={m[1]} terminal_epoch={m[1]}', log),
               log.replace('ttl=12s', 'ttl=20s')]
        for malformed in bad:
            self.assertNotEqual(log, malformed)
            with self.assertRaises(ValueError):
                controls.verify_log('legacy-retirement-weak-frame-expiry', malformed)

    def test_current_expiry_row_cannot_substitute_for_legacy_servers(self):
        log = (REPO/'docs/scale/domain-retirement-weak-frame-expiry-2026-10-06/native-race/native.log').read_text()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            controls.verify_log('legacy-retirement-weak-frame-expiry', log)
        renamed = log.replace(controls.WEAK_EXPIRY, controls.LEGACY_WEAK_EXPIRY)
        with self.assertRaisesRegex(ValueError, 'legacy peer versions/backend'):
            controls.verify_log('legacy-retirement-weak-frame-expiry', renamed)

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

    def test_existing_retirement_pair_cannot_qualify_combined_weak_frame(self):
        log = (REPO/'docs/scale/domain-runtime-ci-2026-10-06/native-qualification/retirement-server-kill/native.log').read_text()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            controls.verify_log('retirement-weak-frame', log)

    def test_actual_combined_frame_row_rejects_wrong_object_generation_route_and_counts(self):
        log = (REPO/'docs/scale/domain-retirement-weak-frame-2026-10-06/native-race/native.log').read_text()
        controls.verify_log('retirement-weak-frame', log)
        bad = [log.replace('generation=3 drops=1', 'generation=1 drops=1'),
               log.replace('drops=1 reads=3', 'drops=0 reads=3'),
               log.replace('drops=1 reads=3', 'drops=1 reads=1'),
               log.replace('leader=1 direct=0', 'leader=2 direct=0'),
               log.replace('leader=1 direct=0', 'leader=1 direct=1'),
               log.replace('$JS.WFRETIRE.API.STREAM.MSG.GET.OBJ_WF_BLOB', '$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB'),
               re.sub(r'(weak frame confirmed: object=)step-result-[a-f0-9]{64}', r'\g<1>step-result-'+'0'*64, log),
               re.sub(r'elapsed=[0-9.]+s', 'elapsed=30s', log, count=1)]
        for malformed in bad:
            self.assertNotEqual(log, malformed)
            with self.assertRaises(ValueError):
                controls.verify_log('retirement-weak-frame', malformed)

    def test_ordinary_weak_frame_cannot_substitute_for_expired_owner(self):
        log = (REPO/'docs/scale/domain-retirement-weak-frame-2026-10-06/native-race/native.log').read_text()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            controls.verify_log('retirement-weak-frame-expiry', log)
        renamed = log.replace(controls.WEAK_FRAME, controls.WEAK_EXPIRY)
        with self.assertRaisesRegex(ValueError, 'successor epoch'):
            controls.verify_log('retirement-weak-frame-expiry', renamed)

    def test_actual_expiry_frame_row_requires_expired_ttl_higher_epoch_and_frame_confirmation(self):
        log = (REPO/'docs/scale/domain-retirement-weak-frame-expiry-2026-10-06/native-race/native.log').read_text()
        controls.verify_log('retirement-weak-frame-expiry', log)
        bad = [re.sub(r'prior_epoch=(\d+) terminal_epoch=\d+', lambda m: f'prior_epoch={m[1]} terminal_epoch={m[1]}', log),
               re.sub(r'held=[0-9.]+s ttl=12s', 'held=12s ttl=12s', log),
               log.replace('ttl=12s', 'ttl=20s'),
               log.replace('drops=1 reads=3', 'drops=0 reads=3'),
               log.replace('generation=3 drops=1', 'generation=1 drops=1'),
               log.replace('signal=killed', 'signal=terminated', 1)]
        for malformed in bad:
            self.assertNotEqual(log, malformed)
            with self.assertRaises(ValueError):
                controls.verify_log('retirement-weak-frame-expiry', malformed)

    def test_native_failure_never_qualifies(self):
        for case in controls.CASES:
            for log in ('FAIL\n', '--- SKIP: x\nPASS\n', 'DATA RACE\nPASS\n'):
                with self.subTest(case=case, log=log), self.assertRaises(ValueError):
                    controls.verify_log(case, log)

    def test_accepted_expiry_pair_rejects_slow_heal_epoch_regression_and_missing_kill(self):
        log = (REPO/'docs/scale/domain-runtime-ci-2026-10-06/native-qualification/retirement-server-kill/native.log').read_text()
        controls.verify_log('retirement-server-kill', log)
        malformed = [re.sub(r'elapsed=[0-9.]+s', 'elapsed=30s', log, count=1),
                     log.replace('prior_epoch=54 terminal_epoch=76', 'prior_epoch=54 terminal_epoch=54'),
                     log.replace('signal=killed', 'signal=terminated', 1)]
        for bad in malformed:
            with self.subTest(bad=bad[-100:]), self.assertRaises(ValueError):
                controls.verify_log('retirement-server-kill', bad)
