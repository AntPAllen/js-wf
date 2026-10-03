"""Exercise source attribution against a real isolated Git checkout."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('capture-tier3-clock-source.py').resolve()


class SourceCaptureTest(unittest.TestCase):
    def test_clean_roundtrip_dirty_tree_and_changed_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / 'repo'
            repo.mkdir()

            def git(*args):
                return subprocess.check_output(['git', *args], cwd=repo, stderr=subprocess.PIPE).decode().strip()

            git('init', '-q')
            git('config', 'user.email', 'fixture@example.invalid')
            git('config', 'user.name', 'Fixture')
            source = repo / 'runtime.go'
            source.write_text('package runtime\n')
            (repo / 'go.mod').write_text('module fixture\n')
            git('add', '.')
            git('commit', '-qm', 'initial')
            revision = git('rev-parse', 'HEAD')

            def capture(stage, row):
                return subprocess.run(['python3', str(SCRIPT), '--root', str(root / row),
                                       '--row', row, '--stage', stage], cwd=repo,
                                      capture_output=True, text=True)

            for row in ('worker_clock', 'rolling_upgrade', 'server_clock_ahead', 'server_clock_behind'):
                with self.subTest(row=row):
                    self.assertEqual(capture('before', row).returncode, 0)
                    self.assertEqual(capture('after', row).returncode, 0)
                    proof = json.loads((root / row / f'{row.replace("_", "-")}-source-after.json').read_text())
                    self.assertEqual(proof['revision'], revision)
                    self.assertTrue(proof['clean'])
                    self.assertEqual(proof['files']['runtime.go'], hashlib.sha256(source.read_bytes()).hexdigest())

            source.write_text('package changed\n')
            for row in ('worker_clock', 'rolling_upgrade', 'server_clock_ahead', 'server_clock_behind'):
                result = capture('after', row)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('requires a clean committed source', result.stderr)
            git('add', '.')
            git('commit', '-qm', 'changed')
            for row in ('worker_clock', 'rolling_upgrade', 'server_clock_ahead', 'server_clock_behind'):
                result = capture('after', row)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('source changed during execution', result.stderr)


if __name__ == '__main__':
    unittest.main()
