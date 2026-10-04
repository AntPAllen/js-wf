#!/usr/bin/env python3
"""Compile and execute one retained integration binary for opt-in diagnostics.

The selected Go/Cgo/test input inventory is a superset, not exhaustive assembly,
embed, generated-input or hermetic compiler provenance. Stdlib/module test files
may be selected without being linked. Only Go test JSON goes to stdout.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def run(root, pattern, timeout, race, selected_package="./integration"):
    if selected_package not in ("./integration", "./integrity"):
        raise ValueError("unsupported retained test package")
    repo = Path.cwd().resolve()
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    if subprocess.check_output(['git', 'status', '--porcelain']):
        raise ValueError('retained execution requires clean committed source')
    if root.is_relative_to(repo):
        raise ValueError('retained artifacts must be outside the checkout')
    root.mkdir(parents=True, exist_ok=False)
    (root/'runner.py').write_bytes(Path(__file__).read_bytes())
    (root/'source.txt').write_text(revision+'\n')
    fmt = '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
    dependencies = subprocess.check_output(['go', 'list', '-deps', '-test', '-f', fmt, selected_package], text=True)
    (root/'dependencies.txt').write_text(dependencies)
    package = subprocess.check_output(['go', 'list', '-f', '{{.ImportPath}}', selected_package], text=True).strip()
    goroot = Path(subprocess.check_output(['go', 'env', 'GOROOT'], text=True).strip()).resolve()
    modules = Path(subprocess.check_output(['go', 'env', 'GOMODCACHE'], text=True).strip()).resolve()
    selected = set()
    ignored = []
    for line in dependencies.splitlines():
        directory, *groups = line.split('|')
        for name in ' '.join(groups).split():
            path = Path(directory)/name
            if not directory or not path.is_file():
                ignored.append(str(path))
            else:
                selected.add(path.resolve())
    selected.update(repo/name for name in ('go.mod', 'go.sum') if (repo/name).is_file())
    before = {str(path): sha(path) for path in sorted(selected)}
    local = 0
    captured = {}
    for path in sorted(selected):
        if path.is_relative_to(repo):
            relative = path.relative_to(repo)
            data = subprocess.check_output(['git', 'show', revision+':'+str(relative)])
            if hashlib.sha256(data).hexdigest() != before[str(path)]:
                raise ValueError(f'local input differs from Git: {relative}')
            destination = root/'selected-source/local'/relative
            local += 1
        elif path.is_relative_to(modules):
            destination = root/'selected-source/modules'/path.relative_to(modules)
        elif path.is_relative_to(goroot):
            destination = root/'selected-source/toolchain'/path.relative_to(goroot)
        else:
            destination = root/'selected-source/other'/str(path).lstrip('/')
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
        if sha(destination) != before[str(path)]:
            raise ValueError(f'input changed during capture: {path}')
        captured[str(path)] = str(destination.relative_to(root))
    write(root/'source-before.json', before)
    write(root/'captured-paths.json', captured)
    write(root/'ignored-generated-inputs.json', ignored)
    binary = root/(Path(selected_package).name+'.test')
    build = ['go', 'test', '-p=1']+(['-race'] if race else [])+['-c', '-o', str(binary), selected_package]
    execute = ['go', 'tool', 'test2json', '-t', '-p', package, str(binary), '-test.run='+pattern, '-test.count=1', '-test.timeout='+timeout, '-test.v']
    write(root/'commands.json', dict(source=revision, build=build, run=execute, package=package, build_cwd=str(repo), run_cwd=str(repo/selected_package),
        environment={k:v for k,v in os.environ.items() if k in ('GOCACHE', 'GOTMPDIR', 'GOMEMLIMIT', 'GOMAXPROCS') or k.startswith(('WF_TIER3_', 'TIER3_MATRIX_', 'FAULT_SEED', 'WF_AUDIT_BATCH_'))}))
    with (root/'build.log').open('wb') as log:
        subprocess.run(build, stdout=log, stderr=subprocess.STDOUT, check=True)
    if {str(path):sha(path) for path in selected} != before:
        raise ValueError('selected source changed during build')
    (root/'binary-build-info.txt').write_text(subprocess.check_output(['go', 'version', '-m', str(binary)], text=True))
    digest = sha(binary)
    started = datetime.now(timezone.utc).isoformat()
    begin = time.monotonic()
    # Keep a local copy as well as the existing workflow's tee/render pipeline.
    with (root/'events.jsonl').open('wb') as events, (root/'stderr.log').open('wb') as errors:
        child = subprocess.Popen(execute, cwd=repo/selected_package, stdout=subprocess.PIPE, stderr=errors)
        for line in child.stdout:
            events.write(line)
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        status = child.wait()
    after = {str(path):sha(path) for path in selected}
    write(root/'source-after.json', after)
    valid = before == after and sha(binary) == digest
    write(root/'execution.json', dict(source=revision, started_at=started, finished_at=datetime.now(timezone.utc).isoformat(),
        wall_seconds=time.monotonic()-begin, exit_code=status, selected_inputs=len(before), local_git_inputs=local,
        actual_binary_sha256=digest, source_before_after_identical=before==after, binary_unchanged=sha(binary)==digest,
        input_inventory_scope='Selected Go/Cgo/test/module superset; excludes exhaustive assembly/embed/generated/hermetic proof.',
        scope='Actual executable and selected inputs retained. Test outcome still requires independent named-test/raw/store review; no parent/full-matrix/24h qualification.'))
    if not valid:
        raise ValueError('selected source or actual executable changed during execution')
    return status


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--test', required=True)
    parser.add_argument('--timeout', default='20m')
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--package', choices=('integration', 'integrity'), default='integration')
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve(), args.test, args.timeout, args.race, './'+args.package))
