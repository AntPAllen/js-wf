#!/usr/bin/env python3
"""Restore a downloaded complete S3 proof using its committed metadata."""
import argparse
import hashlib
import json
from pathlib import Path

import fixture_archive


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--archive", type=Path, required=True)
    p.add_argument("--metadata", type=Path, required=True)
    p.add_argument("--inventory", type=Path, required=True)
    p.add_argument("--destination", type=Path, required=True)
    a = p.parse_args()
    meta = json.loads(a.metadata.read_bytes())
    data = a.inventory.read_bytes()
    if (meta.get("schema") != fixture_archive.SCHEMA
            or hashlib.sha256(data).hexdigest() != meta["inventory_sha256"]):
        raise ValueError("inventory must match complete canonical proof metadata")
    report = fixture_archive.restore(a.archive,
        {"bytes": meta["archive_bytes"], "sha256": meta["archive_sha256"]},
        json.loads(data), a.destination)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
