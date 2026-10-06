import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import fixture_archive


class FixtureArchiveControls(unittest.TestCase):
    def test_complete_capture_and_corrupt_member_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "root"
            root.mkdir()
            (root / "partial.json.tmp").write_bytes(b'{"unfinished":')
            (root / "empty").write_bytes(b"")
            (root / "sdk").write_bytes(b"observed binary")
            (root / "sdk").chmod(0o755)
            archive = base / "proof.tar.gz"
            proof = fixture_archive.capture(root, archive, base / "metadata")
            declared = fixture_archive.verify(archive)
            self.assertEqual(len(declared["files"]), 3)
            self.assertTrue(proof["all_current_fixture_files_unchanged_after_capture"])
            self.assertEqual(declared["files"]["sdk"]["mode"], 0o755)
            self.assertFalse(list((base / "metadata").glob("*.part-*")))
            # Keep the declared hashes while changing one archive member.
            bad = base / "bad.tar.gz"
            with tarfile.open(archive) as src, tarfile.open(bad, "w:gz") as dst:
                for member in src:
                    data = src.extractfile(member).read()
                    if member.name == "sdk":
                        data = b"unobserved binary"
                        member.size = len(data)
                    dst.addfile(member, io.BytesIO(data))
            with self.assertRaisesRegex(ValueError, "differs from inventory"):
                fixture_archive.verify(bad)

    def test_symlink_and_in_fixture_output_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "root"
            root.mkdir()
            (root / "link").symlink_to("/etc/passwd")
            with self.assertRaisesRegex(ValueError, "symlink"):
                fixture_archive.inventory(root)
            with self.assertRaisesRegex(ValueError, "outside fixture"):
                fixture_archive.capture(root, root / "proof.tar.gz", Path(temp) / "meta")

    def test_duplicate_control_and_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            for name in ["../outside", fixture_archive.CONTROL]:
                path = Path(temp) / "bad.tar.gz"
                with tarfile.open(path, "w:gz") as archive:
                    data = json.dumps({"schema": fixture_archive.SCHEMA, "files": {}}).encode()
                    for _ in range(2):
                        member = tarfile.TarInfo(name)
                        member.size = len(data)
                        archive.addfile(member, io.BytesIO(data))
                with self.assertRaises(ValueError):
                    fixture_archive.verify(path)


if __name__ == "__main__":
    unittest.main()
