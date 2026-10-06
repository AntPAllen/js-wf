import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import fixture_archive


class FixtureArchiveControls(unittest.TestCase):
    def test_restore_exact_bytes_modes_and_nanosecond_mtimes(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "root"
            root.mkdir()
            for name, data, mode in [("nested/sdk", b"binary", 0o755),
                                     ("partial.json.tmp", b'{"unfinished":', 0o600),
                                     ("empty", b"", 0o644)]:
                path = root / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(data)
                path.chmod(mode)
                os.utime(path, ns=(1728000000123456789, 1728000000123456789))
            archive = base / "proof.tar.gz"
            proof = fixture_archive.capture(root, archive, base / "meta", compresslevel=1)
            manifest = fixture_archive.verify(archive)
            expected = dict(bytes=proof["archive_bytes"], sha256=proof["archive_sha256"])
            result = fixture_archive.restore(archive, expected, manifest, base / "fresh")
            self.assertTrue(result["all_bytes_modes_mtimes_verified"])
            self.assertEqual(fixture_archive.inventory(base / "fresh"), manifest["files"])
            with self.assertRaisesRegex(ValueError, "fresh"):
                fixture_archive.restore(archive, expected, manifest, root)
            self.assertEqual(fixture_archive.inventory(root), manifest["files"])
            with self.assertRaisesRegex(ValueError, "committed proof"):
                fixture_archive.restore(archive, dict(expected, sha256="0" * 64),
                                        manifest, base / "wrong-hash")
            self.assertFalse((base / "wrong-hash").exists())
            with self.assertRaisesRegex(ValueError, "canonical manifest"):
                fixture_archive.restore(archive, expected, dict(manifest, scope="wrong"),
                                        base / "wrong-inventory")
            self.assertFalse((base / "wrong-inventory").exists())

    def test_restore_detects_archive_path_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "root"
            root.mkdir()
            (root / "data").write_bytes(b"original")
            archive = base / "proof.tar.gz"
            proof = fixture_archive.capture(root, archive, base / "meta")
            manifest = fixture_archive.verify(archive)
            expected = dict(bytes=proof["archive_bytes"], sha256=proof["archive_sha256"])
            real_utime = os.utime

            def replace_archive(*args, **kwargs):
                replacement = base / "replacement"
                replacement.write_bytes(archive.read_bytes())
                replacement.replace(archive)
                return real_utime(*args, **kwargs)

            with mock.patch.object(fixture_archive.os, "utime", side_effect=replace_archive):
                with self.assertRaisesRegex(ValueError, "archive path changed"):
                    fixture_archive.restore(archive, expected, manifest, base / "fresh")

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

    def test_nonseeking_stream_and_complete_compressed_digest(self):
        class NonSeeking:
            def __init__(self, data):
                self.stream = io.BytesIO(data)

            def read(self, size=-1):
                return self.stream.read(min(size, 257) if size >= 0 else 257)

        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "root"
            root.mkdir()
            (root / "data").write_bytes(b"complete archived store" * 1000)
            archive = base / "proof.tar.gz"
            fixture_archive.capture(root, archive, base / "meta")
            data = archive.read_bytes()
            expected = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
            manifest, actual = fixture_archive.verify_hashed_stream(NonSeeking(data), expected)
            self.assertEqual(actual, expected)
            self.assertEqual(manifest, fixture_archive.verify(archive))
            for changed in (data + b"uncommitted trailing bytes", data[:-8]):
                with self.assertRaises((ValueError, tarfile.TarError, EOFError)):
                    fixture_archive.verify_hashed_stream(NonSeeking(changed), expected)
            for wrong in (dict(expected, bytes=len(data)+1), dict(expected, sha256="0"*64)):
                with self.assertRaisesRegex(ValueError, "committed proof"):
                    fixture_archive.verify_hashed_stream(NonSeeking(data), wrong)

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
