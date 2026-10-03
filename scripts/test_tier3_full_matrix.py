import copy
import importlib.util
from pathlib import Path
import unittest
from unittest import mock
import tempfile
import json

spec = importlib.util.spec_from_file_location('full', Path(__file__).with_name('check-tier3-full-matrix.py'))
full = importlib.util.module_from_spec(spec)
spec.loader.exec_module(full)


def fixture(count):
    jobs = full.planner.campaign('all', count)
    source = 'a'*40
    metadata = dict(headSha=source, status='completed', conclusion='success', jobs=[
        dict(name=name, status='completed', conclusion='success') for name in
        ['seeds', *(full.job_name(j) for j in jobs)]])
    logs = {full.job_name(j): source+'\n'+'\n'.join(
        f"TIER3_CAMPAIGN row={j['row']} seed={seed} duration=10m"
        for seed in range(j['first'], j['last']+1)) for j in jobs}
    reports = {row: {seed: dict(seed=seed, duration_seconds=600, shortened_smoke=False,
        clears_full_tier3_release=False, invocations=56, journal_entries=618, confirmed_faults=19,
        common_timer_clock='utc-quorum-v1',
        common_timer_clock_artifact_checks=dict(physical_probes=5),
        clock_timer_cut_artifact_checks=dict(all_cuts_remove_source_before_duration=True),
        checkpoint_audit_checks=dict(all_expected_checkpoints_present=True))
        for seed in range(1, count+1)} for row in full.planner.ROWS}
    return metadata, logs, reports


class FullMatrixTests(unittest.TestCase):
    def test_requested_upgrade_profiles_require_matching_proofs(self):
        metadata, logs, reports = fixture(20)
        with self.assertRaises(ValueError):
            full.check(metadata, logs, reports, 20, '10m', True, True, 'sigkill')
        for record in reports['rolling_upgrade'].values():
            record.update(upgrade_shutdown_mode='sigkill', upgrade_start_gap='cohort-short-v1',
                          start_scan_progress_checks=dict(gaps_with_progress=19))
        result = full.check(metadata, logs, reports, 20, '10m', True, True, 'sigkill')
        self.assertTrue(result['requires_upgrade_start_gap'])
        self.assertEqual(result['expected_upgrade_shutdown'], 'sigkill')
        for change in ('mode', 'gap', 'progress', 'count', 'bool'):
            bad = copy.deepcopy(reports)
            record = bad['rolling_upgrade'][20]
            if change == 'mode': record['upgrade_shutdown_mode'] = 'ldm'
            elif change == 'gap': record.pop('upgrade_start_gap')
            elif change == 'progress': record.pop('start_scan_progress_checks')
            elif change == 'count': record['start_scan_progress_checks']['gaps_with_progress'] = 18
            else: record['start_scan_progress_checks']['gaps_with_progress'] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                full.check(metadata, logs, bad, 20, '10m', True, True, 'sigkill')

    def test_upgrade_options_reach_raw_verifier_only_for_upgrade_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture_root = root/'seed-1'/'tier3-mixed-journal'
            fixture_root.mkdir(parents=True)
            events = fixture_root.parent/'tier3-mixed-journal-events.jsonl'
            report = dict(invocations=56)
            for path in (fixture_root.parent/'tier3-mixed-journal-result.json',
                         fixture_root/'event-explanations.json',
                         fixture_root/'fencing-timeline-review.json'):
                path.write_text(json.dumps(report))
            def regenerate(command, **kwargs):
                Path(command[command.index('--output')+1]).write_text(json.dumps(report))
            for row in ('rolling_upgrade', 'journal'):
                with mock.patch.object(full.subprocess, 'run', side_effect=regenerate) as run:
                    full.verify_seed(events, root/'regenerated.json', row, 1, '10m', True, True, 'ldm')
                    command = run.call_args_list[0].args[0]
                    for option in ('--require-upgrade-start-gap', '--require-start-scan-progress',
                                   '--expected-upgrade-shutdown'):
                        self.assertEqual(option in command, row == 'rolling_upgrade')
                    if row == 'rolling_upgrade':
                        self.assertEqual(command[command.index('--expected-upgrade-shutdown')+1], 'ldm')

    def test_complete_ranges_keep_partial_release_scope(self):
        for count in (1, 20, 200):
            report = full.check(*fixture(count), count, '10m', True)
            self.assertEqual(report['executions'], len(full.planner.ROWS)*count)
            self.assertEqual(report['invocations'], len(full.planner.ROWS)*count*56)
            self.assertFalse(report['clears_full_tier3_release'])

    def test_missing_duplicate_failed_live_and_source_jobs(self):
        for change in ('missing', 'duplicate', 'failed', 'live', 'revision', 'seed_cut', 'seed_duplicate'):
            metadata, logs, reports = fixture(20)
            name = next(iter(logs))
            if change == 'missing': metadata['jobs'].pop()
            elif change == 'duplicate': metadata['jobs'][-1] = copy.deepcopy(metadata['jobs'][1])
            elif change == 'failed': metadata['jobs'][-1]['conclusion'] = 'failure'
            elif change == 'live': metadata['status'] = 'in_progress'
            elif change == 'revision': metadata['headSha'] = 'b'*40
            elif change == 'seed_cut': logs[name] = logs[name].replace('seed=12', 'seed=13')
            else: logs[name] += '\n'+logs[name].splitlines()[-1]
            with self.subTest(change=change), self.assertRaises(ValueError):
                full.check(metadata, logs, reports, 20, '10m', True)

    def test_missing_row_seed_checkpoint_clock_and_release_artifacts(self):
        for change in ('row', 'seed', 'checkpoint', 'clock', 'release', 'duration'):
            metadata, logs, reports = fixture(1)
            record = reports['server_clock_ahead'][1]
            if change == 'row': reports.pop('journal')
            elif change == 'seed': reports['journal'].pop(1)
            elif change == 'checkpoint': record.pop('checkpoint_audit_checks')
            elif change == 'clock': record['common_timer_clock'] = None
            elif change == 'release': record['clears_full_tier3_release'] = True
            else: record['duration_seconds'] = 35
            with self.subTest(change=change), self.assertRaises(ValueError):
                full.check(metadata, logs, reports, 1, '10m', True)


if __name__ == '__main__':
    unittest.main()
