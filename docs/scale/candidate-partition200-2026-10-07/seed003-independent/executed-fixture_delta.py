"""Byte-complete fixture archives that reference unchanged committed store files.

The caller must close the fixture before capture. Referenced base archives are
read and verified in full; neither base stores nor NATS servers are opened.
"""
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile


def safe_name(name):
    p = PurePosixPath(name)
    if p.is_absolute() or not name or any(x in (".", "..") for x in name.split("/")):
        raise ValueError("unsafe archive member: " + name)
    return name


def digest(stream):
    h = hashlib.sha256()
    size = 0
    while block := stream.read(1 << 20):
        h.update(block)
        size += len(block)
    return {"bytes": size, "sha256": h.hexdigest()}


class GitParts(io.RawIOBase):
    def __init__(self, repo, revision, metadata_path):
        self.repo, self.revision = repo, revision
        self.base = str(PurePosixPath(metadata_path).parent)
        self.meta = json.loads(self.blob(metadata_path))
        if not self.meta.get("all_archive_members_and_parts_read_back"):
            raise ValueError("base archive was not verified")
        self.index, self.offset, self.block = 0, 0, b""
        self.hash, self.total = hashlib.sha256(), 0

    def blob(self, name):
        return subprocess.check_output(["git", "cat-file", "blob", self.revision + ":" + name], cwd=self.repo)

    def readable(self):
        return True

    def readinto(self, target):
        if self.offset == len(self.block):
            if self.index == len(self.meta["parts"]):
                return 0
            part = self.meta["parts"][self.index]
            self.block = self.blob(self.base + "/" + safe_name(part["file"]))
            if len(self.block) != part["bytes"] or hashlib.sha256(self.block).hexdigest() != part["sha256"]:
                raise ValueError("base part differs from metadata")
            self.hash.update(self.block)
            self.total += len(self.block)
            self.offset, self.index = 0, self.index + 1
        n = min(len(target), len(self.block) - self.offset)
        target[:n] = self.block[self.offset:self.offset + n]
        self.offset += n
        return n

    def verify(self):
        if self.index != len(self.meta["parts"]) or self.offset != len(self.block) or self.total != self.meta["archive_bytes"] or self.hash.hexdigest() != self.meta["archive_sha256"]:
            raise ValueError("incomplete or invalid base archive")


def read_base(repo, revision, metadata_path, visitor=None):
    raw = GitParts(repo, revision, metadata_path)
    reader = io.BufferedReader(raw)
    actual, declared = {}, None
    with tarfile.open(fileobj=reader, mode="r|gz") as archive:
        for member in archive:
            safe_name(member.name)
            if not member.isfile():
                raise ValueError("base contains a non-file member")
            data = archive.extractfile(member)
            if member.name == "archive-manifest.json":
                if declared is not None:
                    raise ValueError("duplicate base manifest")
                declared = json.load(data)
            else:
                if member.name in actual:
                    raise ValueError("duplicate base member")
                if visitor is None:
                    actual[member.name] = digest(data)
                else:
                    actual[member.name] = visitor(member.name, data)
    while reader.read(1 << 20):
        pass
    raw.verify()
    if declared != actual:
        raise ValueError("base member inventory differs from its manifest")
    return actual


def capture(root, out, repo, revision, metadata_path, root_prefix, base_prefix):
    root, out = Path(root), Path(out)
    # Resolve a stable full commit before writing any reference.
    revision = subprocess.check_output(["git", "rev-parse", revision + "^{commit}"], cwd=repo, text=True).strip()
    base = read_base(repo, revision, metadata_path)
    files, aliases = {}, {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("fixture contains a symlink")
        if not path.is_file():
            continue
        name = safe_name(path.relative_to(root).as_posix())
        if name in ("lossless-manifest.json", "proof-delta.tar.gz"):
            continue
        with path.open("rb") as stream:
            record = digest(stream)
        st = path.stat()
        record.update(mode=st.st_mode & 0o777, mtime_ns=st.st_mtime_ns)
        files[name] = record
        if name.startswith(root_prefix):
            target = safe_name(base_prefix + name[len(root_prefix):])
            if base.get(target) == {k: record[k] for k in ("bytes", "sha256")}:
                aliases[name] = target
    raw = GitParts(repo, revision, metadata_path)
    manifest = {"schema": "js-wf-lossless-fixture-delta-v1", "base": {"revision": revision, "metadata_path": metadata_path, "archive_sha256": raw.meta["archive_sha256"]}, "files": files, "aliases": aliases, "scope": "Complete file bytes, paths, modes and mtimes; base archive plus delta required. Ownership/directory metadata are not restored."}
    (root / "lossless-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    archive_path = root / "proof-delta.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        for name in files:
            if name not in aliases:
                archive.add(root / name, arcname=name, recursive=False)
        archive.add(root / "lossless-manifest.json", arcname="lossless-manifest.json")
    verify(archive_path, repo)
    out.mkdir(parents=True, exist_ok=True)
    parts = []
    with archive_path.open("rb") as stream:
        index = 0
        while block := stream.read(25 * 1024 * 1024):
            path = out / f"proof-delta.tar.gz.part-{index:03d}"
            path.write_bytes(block)
            parts.append({"file": path.name, "bytes": len(block), "sha256": hashlib.sha256(block).hexdigest()})
            index += 1
    with archive_path.open("rb") as stream:
        metadata = digest(stream)
    combined = hashlib.sha256()
    for part in parts:
        block = (out / part["file"]).read_bytes()
        if len(block) != part["bytes"] or hashlib.sha256(block).hexdigest() != part["sha256"]:
            raise ValueError("delta part readback failed")
        combined.update(block)
    if combined.hexdigest() != metadata["sha256"]:
        raise ValueError("delta concatenation failed")
    proof = {"archive_sha256": metadata["sha256"], "archive_bytes": metadata["bytes"], "members": len(files) - len(aliases) + 1, "parts": parts, "all_archive_members_and_parts_read_back": True, "lossless_base_plus_delta_verified": True, "logical_files": len(files), "aliased_files": len(aliases), "aliased_bytes": sum(files[x]["bytes"] for x in aliases), "base": manifest["base"]}
    (out / "lossless-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (out / "archive-verification.json").write_text(json.dumps(proof, indent=2) + "\n")
    return proof


def verify(archive_path, repo, destination=None):
    """Verify the complete virtual tree; optionally reconstruct it on disk."""
    destination = Path(destination) if destination is not None else None
    actual = {}
    with tarfile.open(archive_path) as archive:
        entries = archive.getmembers()
        if len({m.name for m in entries}) != len(entries):
            raise ValueError("duplicate delta member")
        for member in entries:
            safe_name(member.name)
            if not member.isfile():
                raise ValueError("non-file delta member")
        manifest = json.load(archive.extractfile("lossless-manifest.json"))
        if manifest["schema"] != "js-wf-lossless-fixture-delta-v1":
            raise ValueError("unknown delta schema")
        expected = manifest["files"]
        aliases = manifest["aliases"]
        if not set(aliases) <= set(expected):
            raise ValueError("undeclared alias")

        def record(name, stream):
            safe_name(name)
            if destination is None:
                return digest(stream)
            path = destination / name
            if path.exists() or any(p.is_symlink() for p in path.parents):
                raise ValueError("restore destination exists or has a symlink")
            path.parent.mkdir(parents=True, exist_ok=True)
            h, size = hashlib.sha256(), 0
            with path.open("xb") as output:
                while block := stream.read(1 << 20):
                    output.write(block)
                    h.update(block)
                    size += len(block)
            path.chmod(expected[name]["mode"])
            import os
            os.utime(path, ns=(expected[name]["mtime_ns"], expected[name]["mtime_ns"]))
            return {"bytes": size, "sha256": h.hexdigest()}

        for member in entries:
            if member.name == "lossless-manifest.json":
                continue
            if member.name not in expected or member.name in aliases:
                raise ValueError("unexpected or shadowed delta member")
            actual[member.name] = record(member.name, archive.extractfile(member))
        targets = {}
        for name, target in aliases.items():
            safe_name(name)
            safe_name(target)
            targets.setdefault(target, []).append(name)

        def base_member(name, stream):
            names = targets.get(name, [])
            if not names:
                return digest(stream)
            if destination is None:
                value = digest(stream)
                for alias in names:
                    actual[alias] = value
                return value
            # Store-prefix mappings are injective, keeping reconstruction
            # streaming without holding a potentially huge member in memory.
            if len(names) != 1:
                raise ValueError("non-injective restore alias")
            value = record(names[0], stream)
            actual[names[0]] = value
            return value

        base = manifest["base"]
        base_raw = GitParts(repo, base["revision"], base["metadata_path"])
        if base_raw.meta["archive_sha256"] != base["archive_sha256"]:
            raise ValueError("base identity differs")
        read_base(repo, base["revision"], base["metadata_path"], base_member)
        if actual != {name: {k: x[k] for k in ("bytes", "sha256")} for name, x in expected.items()}:
            raise ValueError("reconstructed file inventory differs")
    return {"logical_files": len(actual), "aliased_files": len(aliases), "complete_virtual_tree_verified": True}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path, help="combined proof-delta.tar.gz")
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="Git repository containing the pinned base archive")
    parser.add_argument("--restore", type=Path, help="optionally reconstruct into a new destination")
    args = parser.parse_args()
    print(json.dumps(verify(args.archive, args.repo, args.restore), indent=2))
