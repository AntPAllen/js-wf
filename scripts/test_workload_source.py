"""Exercise source checkout against real Git files and dirty-tree controls."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "workload_source", Path(__file__).with_name("materialize-workload-source.py"))
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


class WorkloadCheckoutTest(unittest.TestCase):
    def test_sparse_expansion_retains_assets_and_verifiers_without_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "checkout"
            root.mkdir()
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root).decode().strip()
            git("init", "-q")
            files = {
                "go.mod": "module fixture\n",
                "sim/testdata/pinned.json": '{"seed": 42}\n',
                "worker/embedded.bin": "embedded payload",
                "docs/proof/check.py": "assert True\n",
                "docs/proof/check.go": "package proof\n",
                "docs/proof/job.yml": "name: verifier\n",
                "docs/proof/archive.tar.gz": "retained proof, excluded from workload",
                ".github/workflows/job.yml": "name: test\n",
                "scripts/asset [1]*?.txt": "literal metacharacters",
                "scripts/space and # hash.txt": "literal space and hash",
            }
            for name, data in files.items():
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(data)
            git("add", ".")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "-qm", "Fixture")
            revision = git("rev-parse", "HEAD")
            git("sparse-checkout", "set", ".github", "scripts")
            self.assertFalse((root / "sim/testdata/pinned.json").exists())
            report = Path(tmp) / "report.json"
            source.materialize(root, report)
            expected = set(files) - {"docs/proof/archive.tar.gz"}
            self.assertEqual({str(p.relative_to(root)) for p in root.rglob("*")
                              if p.is_file() and ".git" not in p.parts}, expected)
            for name in expected:
                self.assertEqual((root / name).read_text(), files[name])
            self.assertEqual(git("status", "--porcelain"), "")
            self.assertEqual(git("rev-parse", "HEAD"), revision)
            self.assertEqual(json.loads(report.read_text())["revision"], revision)
            with self.assertRaises(ValueError):
                source.materialize(root, root / "untracked-report.json")
            with self.assertRaises(ValueError):
                source.materialize(root, report)
            (root / "go.mod").write_text("modified input\n")
            with self.assertRaises(ValueError):
                source.materialize(root, Path(tmp) / "dirty.json")
            self.assertFalse((Path(tmp) / "dirty.json").exists())


if __name__ == "__main__":
    unittest.main()
