import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('journal_shard', Path(__file__).with_name('check-tier2-journal-shard.py'))
shard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shard)


def fixture():
    source = 'a'*40
    run = dict(id=10, head_sha=source, status='in_progress')
    job = dict(id=20, run_id=10, head_sha=source, name='leader (journal, 1-2)', status='completed', conclusion='success')
    artifact = dict(id=30, name='matrix-journal-1-2-10m', expired=False, workflow_run=dict(id=10, head_sha=source))
    log = source+'\n'+'\n'.join(f'Sustained matrix row=journal seed={seed} duration=10m' for seed in (1,2))
    return run, job, artifact, log


class JournalShardTests(unittest.TestCase):
    def test_terminal_shard_binding_does_not_require_green_parent(self):
        run, job, artifact, log = fixture()
        for status in ('in_progress', 'completed'):
            run.update(status=status, conclusion='failure')
            self.assertEqual(shard.bind(run, job, artifact, log, 1, 2), 'a'*40)

    def test_substituted_or_incomplete_execution_is_rejected(self):
        changes = [
            ('job', 'run_id', 11), ('job', 'head_sha', 'b'*40),
            ('job', 'name', 'leader (consumer, 1-2)'), ('job', 'status', 'in_progress'),
            ('job', 'conclusion', 'failure'), ('artifact', 'expired', True),
            ('artifact', 'name', 'matrix-journal-3-4-10m'),
            ('origin', 'id', 11), ('origin', 'head_sha', 'b'*40),
        ]
        for target, key, value in changes:
            run, job, artifact, log = fixture()
            objects = dict(job=job, artifact=artifact, origin=artifact['workflow_run'])
            objects[target][key] = value
            with self.subTest(target=target, key=key), self.assertRaises(ValueError):
                shard.bind(run, job, artifact, log, 1, 2)
        for log in (fixture()[3].replace('10m', '35s'), fixture()[3].replace('a'*40, ''),
                    fixture()[3]+'\n'+fixture()[3].splitlines()[-1], fixture()[3].rsplit('\n',1)[0]):
            run, job, artifact, _ = fixture()
            with self.assertRaises(ValueError):
                shard.bind(run, job, artifact, log, 1, 2)

    def test_consumer_binding_rejects_journal_substitution(self):
        run, job, artifact, log = fixture()
        job['name'] = 'leader (consumer, 1-2)'
        artifact['name'] = 'matrix-consumer-1-2-10m'
        log = log.replace('row=journal', 'row=consumer')
        self.assertEqual(shard.bind(run, job, artifact, log, 1, 2, 'consumer'), 'a'*40)
        with self.assertRaises(ValueError):
            shard.bind(run, job, artifact, log, 1, 2)
        with self.assertRaises(ValueError):
            shard.bind(run, job, artifact, log.replace('row=consumer', 'row=journal'), 1, 2, 'consumer')
        with self.assertRaises(ValueError):
            shard.bind(run, job, artifact, log, 1, 2, 'cluster')

    def test_metadata_without_all_raw_inputs_cannot_qualify(self):
        run, job, artifact, log = fixture()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'raw seed evidence'):
                shard.review(run, job, artifact, log, Path(directory), 1, 2)
            (Path(directory)/'matrix-journal-1-test.jsonl').symlink_to('/dev/null')
            with self.assertRaisesRegex(ValueError, 'symlink'):
                shard.review(run, job, artifact, log, Path(directory), 1, 2)


if __name__ == '__main__':
    unittest.main()
