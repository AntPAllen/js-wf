#!/usr/bin/env python3
"""Decode a verified copied component WAL; compare to pinned NATS decoders."""
import argparse
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

import fixture_archive

REPO = Path(__file__).resolve().parents[1]
GROUP = 'S-R3F-BTlony9Q'
CANONICAL = 'docs/scale/lease-partition-component-2026-10-06/raft-debug-reproduction'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    spec = importlib.util.spec_from_file_location('shared', REPO/'scripts/run-domain-runtime-controls.py')
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    def run(command, **kwargs):
        return subprocess.run(command, cwd=REPO, check=True, **kwargs)
    def git(*argv):
        return subprocess.check_output(['git', *argv], cwd=REPO)
    revision = git('rev-parse', 'HEAD').decode().strip()
    assert revision == git('ls-remote', 'origin', 'refs/heads/main').decode().split()[0]
    before = shared.source_inventory(revision)
    root.mkdir()
    def save(name, value):
        (root/name).write_text(json.dumps(value, indent=2)+'\n')
    save('source-before.json', before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO/name, target)
    for name in ['archive-verification.json', 'fixture-inventory.json', 's3-readback.json', 'result.json']:
        relative = CANONICAL+'/'+name
        assert (REPO/relative).read_bytes() == git('cat-file', 'blob', revision+':'+relative)
        shutil.copyfile(REPO/relative, root/name)
    meta = json.loads((root/'archive-verification.json').read_text())
    manifest = json.loads((root/'fixture-inventory.json').read_text())
    donor = Path('/tmp/js-wf-lease-partition-component-raft-debug-20261006.tar.gz')
    baseline = root/'copied-fixture'
    expected = dict(bytes=meta['archive_bytes'], sha256=meta['archive_sha256'])
    save('restoration.json', fixture_archive.restore(donor, expected, manifest, baseline))
    templates = {'decoder': 'inspect-lease-raft-wal', 'adapter': 'lease-raft-reference-adapter', 'reference': 'lease-raft-reference-cli'}
    for name, template in templates.items():
        relative = 'scripts/'+template+'.go.txt'
        data = git('cat-file', 'blob', revision+':'+relative)
        assert data == (REPO/relative).read_bytes()
        (root/(name+'.go')).write_bytes(data)
    module = Path(subprocess.check_output(['go', 'list', '-m', '-f', '{{.Dir}}', 'github.com/nats-io/nats-server/v2'], cwd=REPO, text=True).strip())
    copied = root/'reference-nats-source'
    shutil.copytree(module, copied)
    overlay_path = copied/'server/diagnostic_lease_append.go'
    save('overlay.json', dict(Replace={str(overlay_path): str(root/'adapter.go')}))
    (root/'reference.mod').write_text((REPO/'go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(copied)+'\n')
    shutil.copyfile(REPO/'go.sum', root/'reference.sum')
    options = ['-modfile='+str(root/'reference.mod'), '-overlay='+str(root/'overlay.json')]
    dependencies = {}
    for name, opts in [('decoder', []), ('reference', options)]:
        output = subprocess.check_output(['go', 'list', *opts, '-deps', '-f', '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}', str(root/(name+'.go'))], cwd=REPO, text=True)
        (root/(name+'-dependencies.txt')).write_text(output)
        for line in output.splitlines():
            directory, *groups = line.split('|')
            for filename in ' '.join(groups).split():
                path = Path(directory)/filename
                if path == overlay_path:
                    path = root/'adapter.go'
                if path.is_relative_to(copied):
                    assert shared.sha(path) == shared.sha(module/path.relative_to(copied))
                digest = shared.sha(path)
                target = root/'dependency-inputs'/str(path).lstrip('/')
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
                dependencies[str(path)] = dict(sha256=digest, captured=str(target.relative_to(root)))
    save('dependencies-before.json', dependencies)
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='2GiB', GOWORK='off', GOFLAGS='')
    commands = {}
    for name, opts in [('decoder', []), ('reference', options)]:
        command = ['go', 'build', '-p=1', *opts, '-o', str(root/name), str(root/(name+'.go'))]
        run(command, env=env)
        commands[name] = dict(build=command, binary_sha256=shared.sha(root/name), build_info=subprocess.check_output(['go', 'version', '-m', str(root/name)], text=True))
    save('commands.json', commands)
    all_rows = {}
    for node in range(3):
        rows = []
        for block in sorted((baseline/'originals/cluster'/f'node-{node}'/'jetstream/$SYS/_js_'/GROUP/'msgs').glob('*.blk')):
            command = [str(root/'decoder'), GROUP, str(block)]
            output = run(command, capture_output=True).stdout
            current = [json.loads(line) for line in output.splitlines()]
            for row in current:
                row['block'] = str(block.relative_to(baseline))
            rows.extend(current)
        active = [row for row in rows if 'append' in row]
        data = ''.join(row['raw_append_base64']+'\n' for row in active).encode()
        upstream = [json.loads(line) for line in run([str(root/'reference')], input=data, capture_output=True).stdout.splitlines()]
        assert upstream == [row['append'] for row in active]
        (root/f'node-{node}.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
        all_rows[node] = active
    minority = all_rows[2]
    assert [row['wal_sequence'] for row in minority] == list(range(3168, 3536))
    assert all(row['checksum_verified'] and row['append']['term'] == 1 and row['append']['commit'] == 3167 and row['append']['leader'] == '0WED9nmv' for row in minority)
    assert all(row['append']['previous_index'] == row['wal_sequence']-1 for row in minority)
    operations = Counter(entry.get('operation') for row in minority for entry in row['append']['entries'])
    assert operations == Counter({4: 2933, 6: 2949})
    for row in minority:
        for entry in row['append']['entries']:
            if entry['operation'] == 4:
                value = entry['stream']
                assert value['headers'] == 'NATS/1.0\r\nNats-Marker-Reason: MaxAge\r\nNats-TTL: 1m0s\r\nNats-Rollup: sub\r\n\r\n' and value['message_bytes'] == 0 and not value['reply']
            else:
                assert entry['deleted']['no_erase'] is True and entry['deleted']['stream'] == 'KV_WF_LEASE'
    control = root/'checksum-control'
    control.mkdir()
    block = baseline/minority[0]['block']
    original = block.read_bytes()
    broken = bytearray(original)
    broken[22] ^= 1
    (control/'2.blk').write_bytes(broken)
    bad = subprocess.run([str(root/'decoder'), GROUP, str(control/'2.blk')], capture_output=True)
    assert bad.returncode != 0 and b'checksum mismatch' in bad.stderr
    (root/'checksum-control.stderr').write_bytes(bad.stderr)
    assert fixture_archive.inventory(baseline) == manifest['files']
    for path, record in dependencies.items():
        assert shared.sha(path) == shared.sha(root/record['captured']) == record['sha256']
    for name in ['decoder', 'reference']:
        assert shared.sha(root/name) == commands[name]['binary_sha256']
    save('dependencies-after.json', dependencies)
    after = shared.source_inventory(revision)
    assert after == before
    save('source-after.json', after)
    save('closure.json', shared.closure(root))
    save('review.json', dict(source=revision, copied_fixture_unchanged=True, original_broker_opened=False, broker_started=False, upstream_reference_append_records=sum(map(len, all_rows.values())), minority_records=len(minority), minority_sequence_range=[3168, 3535], carried_commit=3167, marker_proposals=2933, delete_proposals=2949, first_timestamp_ns=minority[0]['timestamp_ns'], last_timestamp_ns=minority[-1]['timestamp_ns'], checksum_corruption_rejected=True, native_matrix_qualified=False, scope='Offline retained-tail diagnosis. Does not establish a recovery fix, full protocol linearizability or causal Tier1 reproduction.'))
    shutil.copyfile(__file__, root/'executed-review.py')
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps(proof), flush=True)


if __name__ == '__main__':
    main()
