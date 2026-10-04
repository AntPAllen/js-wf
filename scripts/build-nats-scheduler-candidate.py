#!/usr/bin/env python3
"""Build the narrow NATS 2.15 scheduler cleanup candidate outside the checkout.

This produces a diagnostic binary and captured sources, not release qualification.
It leaves the pinned dependency and module cache unchanged.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

REPO = Path(__file__).resolve().parents[1]


def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file()}


def patch(raw):
    anchor = '\tfs.scheduling.running = true\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages('
    if raw.count(anchor) != 1:
        raise ValueError('pinned scheduler anchor differs')
    result = raw.replace(anchor, '\tfs.scheduling.running = true\n\tpriorScheduleCount := len(fs.scheduling.schedules)\n\n\tscheduledMsgs := fs.scheduling.getScheduledMessages(')
    start = result.index('func (fs *fileStore) runMsgScheduling()')
    point = result.index('\n\tif len(scheduledMsgs) > 0 {', start)
    return result[:point]+'\n\tif len(fs.scheduling.schedules) != priorScheduleCount {\n\t\tfs.dirty++\n\t}\n'+result[point:]


def write(path, data):
    path.write_text(json.dumps(data, indent=2)+'\n')


def build(root):
    root = root.resolve()
    if root.exists() or root.is_relative_to(REPO):
        raise ValueError('candidate output must be fresh and outside the checkout')
    module = json.loads(subprocess.check_output(['go', 'list', '-m', '-json', 'github.com/nats-io/nats-server/v2'], cwd=REPO))
    if module['Version'] != 'v2.15.0' or module.get('Replace'):
        raise ValueError('candidate requires the unmodified pinned v2.15.0 module')
    original = Path(module['Dir'])
    before = inventory(original)
    root.mkdir(parents=True)
    (root/'builder.py').write_bytes(Path(__file__).read_bytes())
    write(root/'module.json', module)
    write(root/'module-before.json', before)
    source = root/'nats-source'
    shutil.copytree(original, source)
    if inventory(source) != before:
        raise ValueError('copied upstream bytes differ')
    target = source/'server/filestore.go'
    target.chmod(target.stat().st_mode | 0o200)
    target.write_text(patch(target.read_text()))
    candidate = inventory(source)
    changed = [n for n in before if candidate.get(n) != before[n]]
    if set(candidate) != set(before) or changed != ['server/filestore.go']:
        raise ValueError('candidate changes more than the reviewed scheduler file')
    write(root/'candidate-inventory.json', candidate)
    environment = dict(os.environ, GOWORK='off', GOFLAGS='', CGO_ENABLED='0', GOMAXPROCS='1', GOMEMLIMIT='512MiB')
    fmt = '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}'
    # A copied module is not a Git checkout. Its identity is the pinned module
    # checksum plus complete source inventories, not the VM's parent Git state.
    list_command = ['go', 'list', '-buildvcs=false', '-deps', '-f', fmt, '.']
    listing = subprocess.check_output(list_command, cwd=source, env=environment, text=True)
    (root/'dependencies.txt').write_text(listing)
    inputs = set()
    for line in listing.splitlines():
        directory, go, cgo = line.split('|')
        inputs.update(Path(directory)/name for name in (go+' '+cgo).split())
    inputs.update([source/'go.mod', source/'go.sum'])
    hashes = {str(p): sha(p) for p in sorted(inputs)}
    captures = {}
    for i, p in enumerate(sorted(inputs)):
        destination = root/'selected-source'/f'{i:04d}'/p.name
        destination.parent.mkdir(parents=True)
        shutil.copyfile(p, destination)
        if sha(destination) != hashes[str(p)]:
            raise ValueError('compiled input changed during capture')
        captures[str(p)] = str(destination.relative_to(root))
    write(root/'source-before.json', hashes)
    write(root/'captured-paths.json', captures)
    binary = root/'nats-server'
    command = ['go', 'build', '-buildvcs=false', '-p=1', '-o', str(binary), '.']
    write(root/'command.json', dict(command=command, list_command=list_command, cwd=str(source), environment={k:environment[k] for k in ['GOWORK', 'GOFLAGS', 'CGO_ENABLED', 'GOMAXPROCS', 'GOMEMLIMIT']}, revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()))
    with (root/'build.log').open('wb') as log:
        subprocess.run(command, cwd=source, env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
    after = {str(p): sha(p) for p in inputs}
    write(root/'source-after.json', after)
    write(root/'module-after.json', inventory(original))
    if after != hashes or inventory(original) != before or inventory(source) != candidate:
        raise ValueError('candidate or original module inputs changed during build')
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    (root/'binary-build-info.txt').write_text(info)
    version = subprocess.check_output([str(binary), '-v'], text=True).strip()
    write(root/'binary.json', dict(path=str(binary), sha256=sha(binary), bytes=binary.stat().st_size, version=version, selected_inputs=len(inputs), changed_upstream_files=changed, source_before_after_identical=True, original_module_unchanged=True, scope='Diagnostic candidate only. Selected Go/module inputs retained; not exhaustive assembly/embed/hermetic provenance. No server workload or million-timer/24h gate qualified.'))
    print((root/'binary.json').read_text())


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    build(p.parse_args().root)
