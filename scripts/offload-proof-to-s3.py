#!/usr/bin/env python3
"""Copy a committed archive proof to S3 and verify complete remote bytes.

Uses curl's documented SigV4 support; does not delete any local files. Credentials
come from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (and optional session token).
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote, urlsplit


def digest(stream):
    result, size = hashlib.sha256(), 0
    while block := stream.read(1 << 20):
        result.update(block)
        size += len(block)
    return {"bytes": size, "sha256": result.hexdigest()}


def credentials():
    # Supply credentials through curl's stdin config, never command arguments.
    access, secret = os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]
    token = os.environ.get("AWS_SESSION_TOKEN")
    if any("\n" in v or "\r" in v for v in (access, secret, token or "")):
        raise ValueError("invalid credential line break")
    config = "user = " + json.dumps(access + ":" + secret) + "\n"
    if token:
        config += "header = " + json.dumps("x-amz-security-token: " + token) + "\n"
    return config.encode()


def upload_and_verify(path, url, config, region, expected):
    base = ["curl", "--config", "-", "--aws-sigv4", "aws:amz:" + region + ":s3",
            "--silent", "--show-error", "--fail", "--connect-timeout", "30", "--max-time", "1800"]
    put = subprocess.run(base + ["--upload-file", str(path), "--header", "If-None-Match: *",
                                "--output", "/dev/null", "--write-out", "%{http_code}", url],
                         input=config, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    status = put.stdout.decode().strip()
    if not (put.returncode == 0 and status in ("200", "201", "204")) and not (put.returncode == 22 and status == "412"):
        raise RuntimeError("S3 upload failed: HTTP " + status + ", curl " + str(put.returncode))
    with subprocess.Popen(base + [url], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as get:
        get.stdin.write(config)
        get.stdin.close()
        actual = digest(get.stdout)
        error = get.stderr.read()
        code = get.wait()
    if code:
        raise RuntimeError("S3 readback failed: curl " + str(code))
    if actual != expected:
        raise ValueError("S3 full readback differs from committed proof")
    return {"url": url, "put_http_status": status, "full_readback": actual}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--canonical-metadata", required=True, help="Repository-relative committed archive-verification.json")
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="js-wf/proofs")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme != "https" or not endpoint.netloc or endpoint.username or endpoint.query or endpoint.fragment or endpoint.path not in ("", "/"):
        raise ValueError("endpoint must be an HTTPS origin")
    if not args.bucket or "/" in args.bucket or any(p in ("", ".", "..") for p in args.prefix.split("/")):
        raise ValueError("invalid bucket/prefix")
    if args.archive.is_symlink() or not args.archive.is_file() or args.receipt.exists():
        raise ValueError("archive must be a regular file and receipt must be new")
    repo = Path(__file__).resolve().parent.parent
    revision = subprocess.check_output(["git", "rev-parse", args.revision + "^{commit}"], cwd=repo, text=True).strip()
    metadata_path = repo / args.canonical_metadata
    if metadata_path.is_symlink() or not metadata_path.resolve().is_relative_to(repo):
        raise ValueError("metadata must be inside repository")
    metadata_bytes = subprocess.check_output(["git", "cat-file", "blob", revision + ":" + args.canonical_metadata], cwd=repo)
    if metadata_path.read_bytes() != metadata_bytes:
        raise ValueError("working metadata differs from committed bytes")
    meta = json.loads(metadata_bytes)
    if not meta.get("all_archive_members_and_parts_read_back"):
        raise ValueError("requires a fully verified canonical archive")
    expected = {"bytes": meta["archive_bytes"], "sha256": meta["archive_sha256"]}
    with args.archive.open("rb") as stream:
        if digest(stream) != expected:
            raise ValueError("local archive differs from committed proof")
    prefix = args.prefix + "/" + expected["sha256"]
    origin = args.endpoint.rstrip("/") + "/" + quote(args.bucket, safe="") + "/"
    archive_key = prefix + "/proof.tar.gz"
    metadata_hash = hashlib.sha256(metadata_bytes).hexdigest()
    metadata_key = prefix + "/archive-verification-" + metadata_hash + ".json"
    config = credentials()
    archive = upload_and_verify(args.archive, origin + quote(archive_key, safe="/"), config, args.region, expected)
    metadata = upload_and_verify(metadata_path, origin + quote(metadata_key, safe="/"), config, args.region,
                                 {"bytes": len(metadata_bytes), "sha256": metadata_hash})
    # Recheck the donor after transfers. No deletion is part of this command.
    with args.archive.open("rb") as stream:
        if digest(stream) != expected:
            raise ValueError("local archive changed during transfer")
    if metadata_path.read_bytes() != metadata_bytes:
        raise ValueError("metadata changed during transfer")
    report = {"revision": revision, "canonical_metadata": args.canonical_metadata,
              "endpoint": args.endpoint, "bucket": args.bucket, "archive_key": archive_key,
              "metadata_key": metadata_key, "archive": archive, "metadata": metadata,
              "verified_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "scope": "Full remote byte readback matches committed fully verified archive and metadata; no local deletion, public sharing, retention policy or independent remote durability guarantee."}
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"archive_key": archive_key, "bytes": expected["bytes"], "full_remote_readback_verified": True}))


if __name__ == "__main__":
    main()
