import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('tier3_campaign', Path(__file__).with_name('check-tier3-campaign.py'))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


class CampaignChecks(unittest.TestCase):
    def setUp(self):
        self.metadata = dict(headSha='a' * 40, status='completed', conclusion='success',
                             jobs=[dict(name=name, status='completed', conclusion='success')
                                   for name in ['seeds', *(f'journal ({seed})' for seed in range(1, 21))]])
        self.reports = {seed: dict(seed=seed, duration_seconds=600, shortened_smoke=False,
                                  clears_full_tier3_release=False, invocations=700, journal_entries=7704,
                                  confirmed_faults=19, common_timer_clock='utc-quorum-v1',
                                  clock_timer_cut_artifact_checks=dict(all_cuts_remove_source_before_duration=True),
                                  common_timer_clock_artifact_checks=dict(physical_probes=5),
                                  checkpoint_audit_checks=dict(all_expected_checkpoints_present=True))
                        for seed in range(1, 21)}

    def check(self):
        return campaign.check_campaign(self.metadata, self.reports, 'server_clock_ahead', 20, '10m', True)

    def test_complete_requested_range_remains_partial_release(self):
        result = self.check()
        self.assertEqual((result['invocations'], result['confirmed_faults']), (14000, 380))
        self.assertFalse(result['clears_full_tier3_release'])

    def test_missing_duplicate_failed_and_running_jobs(self):
        original = copy.deepcopy(self.metadata)
        for change in ('missing', 'duplicate', 'extra', 'failed', 'running', 'source', 'live_campaign'):
            with self.subTest(change=change):
                self.metadata = copy.deepcopy(original)
                if change == 'missing': self.metadata['jobs'].pop()
                elif change == 'duplicate': self.metadata['jobs'][-1] = self.metadata['jobs'][1]
                elif change == 'extra': self.metadata['jobs'].append(dict(name='unexpected'))
                elif change == 'failed': self.metadata['jobs'][-1]['conclusion'] = 'failure'
                elif change == 'running': self.metadata['jobs'][-1]['status'] = 'in_progress'
                elif change == 'source': self.metadata['headSha'] = ''
                else: self.metadata['status'] = 'in_progress'
                with self.assertRaises(ValueError): self.check()

    def test_incomplete_or_wrong_artifact_proof(self):
        original = copy.deepcopy(self.reports)
        for change in ('missing', 'extra', 'seed', 'duration', 'release', 'smoke', 'clock', 'cut', 'probes', 'checkpoints'):
            with self.subTest(change=change):
                self.reports = copy.deepcopy(original)
                if change == 'missing': self.reports.pop(20)
                elif change == 'extra': self.reports[21] = self.reports[20]
                elif change == 'seed': self.reports[20]['seed'] = 1
                elif change == 'duration': self.reports[20]['duration_seconds'] = 35
                elif change == 'release': self.reports[20]['clears_full_tier3_release'] = True
                elif change == 'smoke': self.reports[20]['shortened_smoke'] = True
                elif change == 'clock': self.reports[20]['common_timer_clock'] = None
                elif change == 'cut': self.reports[20].pop('clock_timer_cut_artifact_checks')
                elif change == 'probes': self.reports[20]['common_timer_clock_artifact_checks']['physical_probes'] = 4
                else: self.reports[20].pop('checkpoint_audit_checks')
                with self.assertRaises(ValueError): self.check()
