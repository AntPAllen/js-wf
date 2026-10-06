"""Complete closed-fixture archives without local archive-part duplicates.

The caller establishes process/descriptor closure. Capture preserves existing
file bytes, including failed or partial artifacts; it never repairs a verdict.
"""
import hashlib
import json
from pathlib import Path
import tarfile

from fixture_delta import digest, safe_name

CONTROL = "__fixture_inventory_v1__.json"
SCHEMA = "js-wf-full-fixture-archive-v1"


def inventory(root):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("fixture root must be a real directory")
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("fixture contains a symlink")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError("fixture contains a non-regular file")
        name = safe_name(path.relative_to(root).as_posix())
        if name == CONTROL:
            raise ValueError("reserved fixture inventory name")
        with path.open("rb") as stream:
            record = digest(stream)
        stat = path.stat()
        record.update(mode=stat.st_mode & 0o777, mtime_ns=stat.st_mtime_ns)
        files[name] = record
    return files


def verify_stream(stream):
    """Verify every member of a non-seeking compressed input stream."""
    actual, declared = {}, None
    with tarfile.open(fileobj=stream, mode="r|gz") as archive:
        for member in archive:
            safe_name(member.name)
            if not member.isfile():
                raise ValueError("archive contains a non-regular member")
            stream = archive.extractfile(member)
            if member.name == CONTROL:
                if declared is not None:
                    raise ValueError("duplicate inventory")
                declared = json.load(stream)
            else:
                if member.name in actual:
                    raise ValueError("duplicate fixture member")
                actual[member.name] = dict(digest(stream), mode=member.mode & 0o777)
    if declared is None or declared.get("schema") != SCHEMA:
        raise ValueError("missing or invalid fixture inventory")
    expected = {name: {k: record[k] for k in ("bytes", "sha256", "mode")}
                for name, record in declared["files"].items()}
    if actual != expected:
        raise ValueError("fixture archive differs from inventory")
    return declared


def verify(archive_path):
    with Path(archive_path).open("rb") as stream:
        return verify_stream(stream)


def verify_hashed_stream(stream, expected):
    """Verify members and the complete compressed body, including trailing bytes."""
    class HashedInput:
        def __init__(self):
            self.hash = hashlib.sha256()
            self.size = 0

        def read(self, size=-1):
            data = stream.read(size)
            self.hash.update(data)
            self.size += len(data)
            return data

    hashed = HashedInput()
    declared = verify_stream(hashed)
    while hashed.read(1 << 20):
        pass
    actual = {"bytes": hashed.size, "sha256": hashed.hash.hexdigest()}
    if actual != expected:
        raise ValueError("complete compressed archive differs from committed proof")
    return declared, actual


def capture(root, archive_path, out):
    root, archive_path, out = Path(root), Path(archive_path), Path(out)
    if archive_path.resolve().is_relative_to(root.resolve()) or out.resolve().is_relative_to(root.resolve()):
        raise ValueError("archive and metadata must be outside fixture")
    out.mkdir(parents=True, exist_ok=True)
    metadata = out / "archive-verification.json"
    manifest_path = out / "fixture-inventory.json"
    if metadata.exists() or manifest_path.exists():
        raise ValueError("proof metadata must be new")
    files = inventory(root)
    declared = {"schema": SCHEMA, "files": files,
                "scope": "Complete file bytes, paths, modes and original nanosecond mtimes; no ownership/directory metadata or gate qualification."}
    # The control JSON is an ordinary final member, never an alias or hardlink.
    with archive_path.open("xb") as raw:
        with tarfile.open(fileobj=raw, mode="w:gz", dereference=True) as archive:
            for name in files:
                archive.add(root / name, arcname=name, recursive=False)
            import io
            data = (json.dumps(declared, indent=2) + "\n").encode()
            member = tarfile.TarInfo(CONTROL)
            member.size, member.mode = len(data), 0o600
            archive.addfile(member, io.BytesIO(data))
    if verify(archive_path) != declared or inventory(root) != files:
        raise ValueError("fixture changed during capture")
    with archive_path.open("rb") as stream:
        archive_digest = digest(stream)
    manifest_bytes = (json.dumps(declared, indent=2) + "\n").encode()
    manifest_path.write_bytes(manifest_bytes)
    proof = {"schema": SCHEMA, "archive_sha256": archive_digest["sha256"],
             "archive_bytes": archive_digest["bytes"], "members": len(files) + 1,
             "all_archive_members_read_back": True,
             "all_current_fixture_files_unchanged_after_capture": True,
             "inventory_file": manifest_path.name,
             "inventory_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
             "storage": "Full archive for S3 content-addressed storage; manifests/receipts in Git. No local or Git archive-part copies."}
    metadata.write_text(json.dumps(proof, indent=2) + "\n")
    return proof
