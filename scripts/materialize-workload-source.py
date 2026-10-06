#!/usr/bin/env python3
"""Materialize the complete workload/verifier input tree without broker archives.

Use a blob-filtered checkout first. This is the same file selection as isolated
VM producers: every non-docs file plus every Go/Python/workflow file in docs.
The report is evidence of checkout selection, never test qualification.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def materialize(checkout, report):
    checkout, report = Path(checkout).resolve(), Path(report).resolve()
    if report.is_relative_to(checkout) or report.exists():
        raise ValueError("report must be new and outside the checkout")
    git = lambda *args: subprocess.check_output(["git", *args], cwd=checkout)
    if Path(git("rev-parse", "--show-toplevel").decode().strip()).resolve() != checkout:
        raise ValueError("require the checkout root")
    if git("status", "--porcelain"):
        raise ValueError("checkout must be clean before source selection")
    revision = git("rev-parse", "HEAD").decode().strip()
    names = git("ls-tree", "-r", "--name-only", "-z", revision).decode().split("\0")[:-1]
    selected = [n for n in names if not n.startswith("docs/") or n.endswith((".go", ".py", ".yml"))]
    if any("\n" in n or "\r" in n for n in selected):
        raise ValueError("unsupported newline in source path")
    # Escape gitignore metacharacters so each entry selects one exact file.
    def pattern(name):
        for char in ("\\", "*", "?", "[", "]"):
            name = name.replace(char, "\\" + char)
        return "/" + name + "\n"
    subprocess.run(["git", "sparse-checkout", "set", "--no-cone", "--stdin"],
                   cwd=checkout, input="".join(map(pattern, selected)), text=True, check=True)
    subprocess.run(["git", "read-tree", "-mu", "HEAD"], cwd=checkout, check=True)
    inputs, size = {}, 0
    for name in selected:
        path = checkout / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("selected input is not a regular file: " + name)
        data = path.read_bytes()
        size += len(data)
        inputs[name] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    if git("status", "--porcelain") or git("rev-parse", "HEAD").decode().strip() != revision:
        raise ValueError("source selection changed the committed checkout")
    result = dict(schema="js-wf-workload-source-checkout-v1", revision=revision,
                  tracked_paths=len(names), materialized_paths=len(selected),
                  materialized_bytes=size, omitted_docs_paths=len(names)-len(selected),
                  inputs=inputs, scope="Complete non-docs tree and Go/Python/workflow verification inputs in docs; same selection as isolated producers. Retained archive blobs excluded from checkout, canonical Git proofs unchanged. No test verdict or reduced workload/seed inventory.")
    report.parent.mkdir(parents=True, exist_ok=True)
    with report.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--checkout", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--report", type=Path, required=True)
    a = p.parse_args()
    result = materialize(a.checkout, a.report)
    print("Verified source checkout", result["revision"], result["materialized_paths"], "files", result["materialized_bytes"], "bytes")
