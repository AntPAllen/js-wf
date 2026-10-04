import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('restore', Path(__file__).with_name(
    'restore-tier3-worker-clock-executables.py'))
restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(restore)


class ExecutableRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        self.proof = self.repo / 'proof'
        self.proof.mkdir()
        source = 'a' * 40
        summary = json.dumps(dict(source=source, job_id=123)).encode()
        files = {'summary.json': summary}
        assets = ['clock-unshifted.test', 'clock-0/integration-clock.test',
                  'clock-1/integration-clock.test']
        for asset in assets:
            files['raw/tier3-matrix-range/seed-131/tier3-mixed-journal/' + asset] = asset.encode()
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w:gz') as tar:
            for name, data in files.items():
                item = tarfile.TarInfo(name)
                item.size = len(data)
                tar.addfile(item, io.BytesIO(data))
        data = stream.getvalue()
        sha = hashlib.sha256(data).hexdigest()
        self.manifest = dict(files={n: dict(sha256=hashlib.sha256(b).hexdigest(), bytes=len(b))
                                    for n, b in files.items()}, archive_sha256=sha,
                             archive_bytes=len(data), parts=[dict(path='part', sha256=sha, bytes=len(data))])
        (self.proof / 'manifest.json').write_text(json.dumps(self.manifest))
        (self.proof / 'summary.json').write_bytes(summary)
        (self.proof / 'part').write_bytes(data)
        self.git('init', '-q')
        self.git('add', '.')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                 'commit', '-qm', 'fixture')
        self.references = dict(provider=dict(revision=self.git('rev-parse', 'HEAD').strip(),
            directory='proof', manifest_sha256=restore.digest(self.proof / 'manifest.json'),
            archive_sha256=sha, source=source, job=123), files={})
        for seed in [144, 145]:
            for asset in assets:
                name = f'raw/tier3-matrix-range/seed-{seed}/tier3-mixed-journal/{asset}'
                provider = f'raw/tier3-matrix-range/seed-131/tier3-mixed-journal/{asset}'
                self.references['files'][name] = dict(provider_member=provider,
                                                       **self.manifest['files'][provider])
        self.destination = self.root / 'destination'

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], text=True)

    def test_exact_restore_then_existing_bytes_verify_without_overwrite(self):
        result = restore.restore(self.references, self.repo, self.destination)
        self.assertEqual((result['destinations_verified'], result['distinct_provider_members'],
                          result['files_created']), (6, 3, 6))
        for name, record in self.references['files'].items():
            self.assertEqual(restore.digest(self.destination / name), record['sha256'])
        repeated = restore.restore(self.references, self.repo, self.destination)
        self.assertEqual(repeated['files_created'], 0)

    def test_wrong_source_pin_or_member_cannot_restore(self):
        for change in ['source', 'revision', 'manifest', 'member']:
            refs = copy.deepcopy(self.references)
            if change == 'source': refs['provider']['source'] = 'b' * 40
            elif change == 'revision': refs['provider']['revision'] = 'HEAD'
            elif change == 'manifest': refs['provider']['manifest_sha256'] = '0' * 64
            else: next(iter(refs['files'].values()))['sha256'] = '0' * 64
            with self.subTest(change=change), self.assertRaises(ValueError):
                restore.restore(refs, self.repo, self.destination)
            self.assertFalse(self.destination.exists())

    def test_corrupted_archive_part_is_rejected_before_destination_writes(self):
        (self.proof / 'part').write_bytes(b'corrupt')
        self.git('add', 'proof/part')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                 'commit', '-qm', 'corrupt archive')
        self.references['provider']['revision'] = self.git('rev-parse', 'HEAD').strip()
        with self.assertRaisesRegex(ValueError, 'part failed verification'):
            restore.restore(self.references, self.repo, self.destination)
        self.assertFalse(self.destination.exists())

    def test_path_escape_symlink_or_changed_existing_executable_is_rejected(self):
        refs = copy.deepcopy(self.references)
        refs['files']['../outside'] = refs['files'].pop(next(iter(refs['files'])))
        with self.assertRaises(ValueError):
            restore.restore(refs, self.repo, self.destination)
        self.destination.symlink_to(self.repo, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            restore.restore(self.references, self.repo, self.destination)
        self.destination.unlink()
        last = self.destination / list(self.references['files'])[-1]
        last.parent.mkdir(parents=True)
        last.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'refusing overwrite'):
            restore.restore(self.references, self.repo, self.destination)
        self.assertEqual(last.read_bytes(), b'changed')
        self.assertFalse((self.destination / next(iter(self.references['files']))).exists())


if __name__ == '__main__':
    unittest.main()
