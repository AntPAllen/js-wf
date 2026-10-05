import hashlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import tarfile
import tempfile
import unittest

import fixture_delta as delta


class FixtureDeltaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.repo = self.home / "repo"
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        self.root = self.home / "fixture"
        self.root.mkdir()
        rng = random.Random(73901)
        self.original = {}
        for n in range(16):
            name = f"copied-stores/node-{n % 3}/block-{n}"
            self.original[name] = rng.randbytes(4096 + n)
            p = self.root / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(self.original[name])
            p.chmod(0o640)
            os.utime(p, ns=(1700000000000000000 + n, 1700000000000000000 + n))
        base = self.home / "base.tar.gz"
        inventory = {}
        with tarfile.open(base, "w:gz") as archive:
            for name, data in self.original.items():
                member = "originals/cluster/" + name.removeprefix("copied-stores/")
                inventory[member] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                info = tarfile.TarInfo(member)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            body = json.dumps(inventory).encode()
            info = tarfile.TarInfo("archive-manifest.json")
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
        raw = base.read_bytes()
        part = self.repo / "proof.part"
        part.write_bytes(raw)
        self.metadata = "base.json"
        (self.repo / self.metadata).write_text(json.dumps({"archive_sha256": hashlib.sha256(raw).hexdigest(), "archive_bytes": len(raw), "all_archive_members_and_parts_read_back": True, "parts": [{"file": part.name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}]}))
        subprocess.run(["git", "add", "."], cwd=self.repo, check=True)
        subprocess.run(["git", "-c", "user.name=Fixture Test", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "base"], cwd=self.repo, check=True)

    def capture(self):
        return delta.capture(self.root, self.home / "out", self.repo, "HEAD", self.metadata, "copied-stores/", "originals/cluster/")

    def test_seeded_modified_added_deleted_and_metadata_reconstruction(self):
        paths = sorted(self.original)
        (self.root / paths[0]).write_bytes(b"changed metadata")
        (self.root / paths[1]).unlink()
        (self.root / "sdk.bin").write_bytes(b"new executed source")
        # Reserved names apply only at fixture root, not to actual data paths.
        (self.root / "copied-stores/proof-delta.tar.gz").write_bytes(b"nested data")
        expected = {p.relative_to(self.root).as_posix(): (p.read_bytes(), p.stat().st_mode & 0o777, p.stat().st_mtime_ns) for p in self.root.rglob("*") if p.is_file()}
        proof = self.capture()
        self.assertEqual(proof["aliased_files"], 14)
        self.assertEqual(proof["logical_files"], len(expected))
        restored = self.home / "restored"
        result = delta.verify(self.root / "proof-delta.tar.gz", self.repo, restored)
        self.assertEqual(result["logical_files"], len(expected))
        actual = {p.relative_to(restored).as_posix(): (p.read_bytes(), p.stat().st_mode & 0o777, p.stat().st_mtime_ns) for p in restored.rglob("*") if p.is_file()}
        self.assertEqual(actual, expected)
        self.assertFalse((restored / paths[1]).exists())
        # Verification reads committed base bytes, not modified local base files.
        (self.repo / "proof.part").write_bytes(b"different working copy")
        self.assertTrue(delta.verify(self.root / "proof-delta.tar.gz", self.repo)["complete_virtual_tree_verified"])

    def rewrite_manifest(self, change):
        path = self.root / "proof-delta.tar.gz"
        with tarfile.open(path) as archive:
            records = [(m, archive.extractfile(m).read()) for m in archive]
        with tarfile.open(path, "w:gz") as archive:
            for member, body in records:
                if member.name == "lossless-manifest.json":
                    manifest = json.loads(body)
                    change(manifest)
                    body = json.dumps(manifest).encode()
                    member.size = len(body)
                archive.addfile(member, io.BytesIO(body))

    def test_wrong_base_reference_and_missing_alias_rejected(self):
        self.capture()
        self.rewrite_manifest(lambda m: m["base"].update(archive_sha256="0" * 64))
        with self.assertRaisesRegex(ValueError, "base identity"):
            delta.verify(self.root / "proof-delta.tar.gz", self.repo)
        self.capture()
        self.rewrite_manifest(lambda m: m["aliases"].update({next(iter(m["aliases"])): "absent/member"}))
        with self.assertRaisesRegex(ValueError, "inventory"):
            delta.verify(self.root / "proof-delta.tar.gz", self.repo)

    def test_traversal_and_existing_restore_destination_rejected(self):
        self.capture()
        self.rewrite_manifest(lambda m: m["aliases"].update({next(iter(m["aliases"])): "../escape"}))
        with self.assertRaisesRegex(ValueError, "unsafe"):
            delta.verify(self.root / "proof-delta.tar.gz", self.repo)
        self.capture()
        destination = self.home / "existing"
        delta.verify(self.root / "proof-delta.tar.gz", self.repo, destination)
        with self.assertRaisesRegex(ValueError, "exists"):
            delta.verify(self.root / "proof-delta.tar.gz", self.repo, destination)

    def test_symlink_fixture_rejected(self):
        (self.root / "link").symlink_to(self.root / next(iter(self.original)))
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.capture()


if __name__ == "__main__":
    unittest.main()
