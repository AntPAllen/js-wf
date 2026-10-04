"""Exercise retained execution with actual compiled Go tests and source mutation."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('run-tier3-retained-test.py').resolve()


class RetainedExecutionTest(unittest.TestCase):
    def test_actual_pass_failure_source_mutation_and_existing_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root/'repo'
            (repo/'integration').mkdir(parents=True)
            (repo/'go.mod').write_text('module retainedfixture\n\ngo 1.27.0\n')
            (repo/'runtime.go').write_text('package retainedfixture\nvar Value = 1\n')
            (repo/'integration/fixture_test.go').write_text('''package integration
import ("os"; "testing"; "retainedfixture")
var _ = retainedfixture.Value
func TestPass(t *testing.T) {}
func TestFail(t *testing.T) { t.Fatal("intentional failure") }
func TestMutate(t *testing.T) {
 if err := os.WriteFile("../runtime.go", []byte("package changed\\n"), 0600); err != nil { t.Fatal(err) }
}
''')
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=repo, stderr=subprocess.PIPE)
            git('init', '-q')
            git('config', 'user.name', 'Fixture')
            git('config', 'user.email', 'fixture@example.invalid')
            git('add', '.')
            git('commit', '-qm', 'fixture')
            for test, expected in [('TestPass', 0), ('TestFail', 1), ('TestMutate', 1)]:
                with self.subTest(test=test):
                    output = root/test
                    result = subprocess.run(['python3', str(SCRIPT), '--root', str(output), '--test', '^'+test+'$', '--timeout', '10s'], cwd=repo, capture_output=True, timeout=120)
                    self.assertEqual(result.returncode, expected, result.stderr.decode())
                    execution = json.loads((output/'execution.json').read_text())
                    self.assertEqual(hashlib.sha256((output/'integration.test').read_bytes()).hexdigest(), execution['actual_binary_sha256'])
                    rows = [json.loads(line) for line in result.stdout.splitlines()]
                    self.assertEqual([r['Test'] for r in rows if r['Action']=='run'], [test])
                    self.assertEqual(result.stdout, (output/'events.jsonl').read_bytes())
                    before = json.loads((output/'source-before.json').read_text())
                    captured = json.loads((output/'captured-paths.json').read_text())
                    for original, path in captured.items():
                        self.assertEqual(hashlib.sha256((output/path).read_bytes()).hexdigest(), before[original])
                    if test == 'TestMutate':
                        self.assertFalse(execution['source_before_after_identical'])
                        self.assertIn(b'source or actual executable changed', result.stderr)
                        git('restore', 'runtime.go')
                    else:
                        self.assertTrue(execution['source_before_after_identical'])
                        self.assertEqual(execution['exit_code'], expected)
                    binary_hash = execution['actual_binary_sha256']
                    refused = subprocess.run(['python3', str(SCRIPT), '--root', str(output), '--test', '^TestPass$'], cwd=repo, capture_output=True, timeout=30)
                    self.assertNotEqual(refused.returncode, 0)
                    self.assertEqual(hashlib.sha256((output/'integration.test').read_bytes()).hexdigest(), binary_hash)


if __name__ == '__main__':
    unittest.main()
