import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('shard', Path(__file__).with_name('check-tier3-matrix-shard.py'))
shard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shard)


def fixture(row='journal', first=1, last=2):
    source = 'a'*40
    span = str(first) if first == last else f'{first}-{last}'
    run = dict(id=10, head_sha=source, status='in_progress', conclusion=None)
    job = dict(id=20, run_id=10, head_sha=source, name=f'journal ({row}, {span})',
               status='completed', conclusion='success')
    artifact = dict(id=30, name=f'tier3-{row}-seed-{span}', expired=False,
                    workflow_run=dict(id=10, head_sha=source))
    log = source+'\n'+'\n'.join(f'TIER3_CAMPAIGN row={row} seed={seed} duration=10m' for seed in range(first,last+1))
    return run, job, artifact, log


class MatrixShardTests(unittest.TestCase):
    def test_terminal_job_can_be_reviewed_without_promoting_failed_parent(self):
        run, job, artifact, log = fixture()
        for status, conclusion in [('in_progress', None), ('completed', 'failure')]:
            run.update(status=status, conclusion=conclusion)
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for seed in (1, 2):
                    folder = root/f'seed-{seed}'
                    folder.mkdir()
                    (folder/'tier3-mixed-journal-events.jsonl').write_text('{}\n')
                def reviewed(path, output, row, seed, duration, *profiles):
                    return dict(seed=seed, invocations=28, journal_entries=308, confirmed_faults=19)
                with mock.patch.object(shard.full, 'verify_seed', side_effect=reviewed) as raw:
                    result = shard.review(run, job, artifact, log, root, 'journal', 1, 2)
                self.assertEqual(raw.call_count, 2)
                self.assertTrue(result['shard_qualified'])
                self.assertEqual(result['invocations'], 56)
                for flag in ('qualifies_parent_campaign', 'qualifies_full_row', 'clears_full_tier3_release'):
                    self.assertFalse(result[flag])

    def test_job_artifact_and_actual_seed_binding_reject_substitutions(self):
        for change in ('failed', 'live', 'wrong_job_run', 'wrong_job_head', 'wrong_name',
                       'wrong_artifact_run', 'wrong_artifact_head', 'wrong_artifact_name',
                       'expired', 'missing_checkout', 'shortened', 'duplicate', 'missing_seed'):
            run, job, artifact, log = fixture()
            if change == 'failed': job['conclusion'] = 'failure'
            elif change == 'live': job['status'] = 'in_progress'
            elif change == 'wrong_job_run': job['run_id'] = 11
            elif change == 'wrong_job_head': job['head_sha'] = 'b'*40
            elif change == 'wrong_name': job['name'] = 'journal (consumer, 1-2)'
            elif change == 'wrong_artifact_run': artifact['workflow_run']['id'] = 11
            elif change == 'wrong_artifact_head': artifact['workflow_run']['head_sha'] = 'b'*40
            elif change == 'wrong_artifact_name': artifact['name'] = 'tier3-journal-seed-3-4'
            elif change == 'expired': artifact['expired'] = True
            elif change == 'missing_checkout': log = log.replace('a'*40, '')
            elif change == 'shortened': log = log.replace('10m', '35s')
            elif change == 'duplicate': log += '\n'+log.splitlines()[-1]
            else: log = '\n'.join(log.splitlines()[:-1])
            with self.subTest(change=change), self.assertRaises(ValueError):
                shard.bind(run, job, artifact, log, 'journal', 1, 2)

    def test_metadata_or_partial_raw_inputs_cannot_qualify_shard(self):
        run, job, artifact, log = fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.object(shard.full, 'verify_seed') as raw:
                with self.assertRaises(ValueError):
                    shard.review(run, job, artifact, log, root, 'journal', 1, 2)
                (root/'seed-1').mkdir()
                (root/'seed-1/tier3-mixed-journal-events.jsonl').write_text('{}\n')
                with self.assertRaises(ValueError):
                    shard.review(run, job, artifact, log, root, 'journal', 1, 2)
                raw.assert_not_called()

    def test_raw_failure_and_original_mutation_do_not_produce_qualification(self):
        run, job, artifact, log = fixture(first=1, last=1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = root/'tier3-mixed-journal-events.jsonl'
            events.write_text('{}\n')
            with mock.patch.object(shard.full, 'verify_seed', side_effect=ValueError('raw counterexample')):
                with self.assertRaisesRegex(ValueError, 'raw counterexample'):
                    shard.review(run, job, artifact, log, root, 'journal', 1, 1)
            def changing(*args):
                events.write_text('changed\n')
                return dict(seed=1, invocations=28, journal_entries=308, confirmed_faults=19)
            with mock.patch.object(shard.full, 'verify_seed', side_effect=changing):
                with self.assertRaisesRegex(ValueError, 'changed during review'):
                    shard.review(run, job, artifact, log, root, 'journal', 1, 1)

    def test_release_profiles_cannot_be_omitted(self):
        for row in ('server_clock_ahead', 'server_clock_behind', 'rolling_upgrade'):
            run, job, artifact, log = fixture(row, 1, 1)
            with tempfile.TemporaryDirectory() as directory, self.subTest(row=row):
                with self.assertRaises(ValueError):
                    shard.review(run, job, artifact, log, Path(directory), row, 1, 1)

    def test_captured_source_must_match_exact_git_inventory(self):
        run, job, artifact, log = fixture('worker_clock', 1, 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'tier3-mixed-journal-events.jsonl').write_text('{}\n')
            evidence = root/'tier3-mixed-journal'
            evidence.mkdir()
            proof = dict(revision=run['head_sha'], clean=True, files={'go.mod':'wrong'})
            for stage in ('before', 'after'):
                (evidence/f'worker-clock-source-{stage}.json').write_text(json.dumps(proof))
            with mock.patch.object(shard, 'source_hashes', return_value={'go.mod':'expected'}):
                with mock.patch.object(shard.full, 'verify_seed') as raw:
                    with self.assertRaisesRegex(ValueError, 'exact Git source'):
                        shard.review(run, job, artifact, log, root, 'worker_clock', 1, 1)
                    raw.assert_not_called()

    def test_required_profiles_are_forwarded_to_raw_review(self):
        for row, clock, gap, mode in [('server_clock_ahead', True, False, None),
                                      ('server_clock_behind', True, False, None),
                                      ('rolling_upgrade', False, True, 'sigkill'),
                                      ('rolling_upgrade', False, True, 'ldm')]:
            run, job, artifact, log = fixture(row, 1, 1)
            with tempfile.TemporaryDirectory() as directory, self.subTest(row=row, mode=mode):
                root = Path(directory)
                (root/'tier3-mixed-journal-events.jsonl').write_text('{}\n')
                evidence = root/'tier3-mixed-journal'
                evidence.mkdir()
                inputs = {'go.mod':'modelled-input'}
                proof = dict(revision=run['head_sha'], clean=True, files=inputs)
                for stage in ('before', 'after'):
                    (evidence/f'{row.replace("_", "-")}-source-{stage}.json').write_text(json.dumps(proof))
                record = dict(seed=1, invocations=28, journal_entries=308, confirmed_faults=19)
                with mock.patch.object(shard, 'source_hashes', return_value=inputs):
                    with mock.patch.object(shard.full, 'verify_seed', return_value=record) as raw:
                        result = shard.review(run, job, artifact, log, root, row, 1, 1, clock, gap, mode)
                self.assertEqual(raw.call_args.args[-3:], (clock, gap, mode))
                self.assertFalse(result['clears_full_tier3_release'])


if __name__ == '__main__':
    unittest.main()
