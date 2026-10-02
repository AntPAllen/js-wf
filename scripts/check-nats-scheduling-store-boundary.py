#!/usr/bin/env python3
"""Isolate failed million-timer replica recovery in pinned NATS file-store code."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestWorkflowCopiedSchedulingIndexBoundary'


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=Path, required=True, help='failed volume campaign root; read only')
    parser.add_argument('--root', type=Path, required=True, help='new diagnostic output root')
    args = parser.parse_args()
    original, out = args.original.resolve(), args.root.resolve()
    report = json.loads((original / 'report.json').read_text())
    assert report['scheduled_count'] == 1000000 and report['status'] == 'failed', 'wrong campaign'
    assert not out.exists() and not out.is_relative_to(original), 'new separate root required'
    module = Path(subprocess.check_output(['go', 'list', '-m', '-f', '{{.Dir}}',
                   'github.com/nats-io/nats-server/v2'], cwd=REPO, text=True).strip())
    version = subprocess.check_output(['go', 'list', '-m', '-f', '{{.Version}}',
                   'github.com/nats-io/nats-server/v2'], cwd=REPO, text=True).strip()
    assert version == 'v2.15.0', 'diagnostic is pinned to original server version'
    out.mkdir(parents=True)
    before = hashes(original)
    (out / 'original-before.json').write_text(json.dumps(before, indent=2) + '\n')
    names = ['server/filestore.go', 'server/scheduler.go', 'go.mod', 'go.sum']
    source = {n: hashlib.sha256((module / n).read_bytes()).hexdigest() for n in names}
    fixture = REPO / 'scripts/fixtures/nats-scheduling-store-boundary_test.go.txt'
    (out / 'fixture.go').write_bytes(fixture.read_bytes())
    (out / 'runner.py').write_bytes(Path(__file__).read_bytes())
    (out / 'source.json').write_text(json.dumps({'version': version, 'module': str(module),
        'module_files': source, 'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'fixture_sha256': hashlib.sha256(fixture.read_bytes()).hexdigest(),
        'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()}, indent=2) + '\n')
    mutation = []
    try:
        for mode in ['intact', 'removed']:
            for node in range(3):
                src = original / 'cluster' / f'node-{node}' / 'jetstream' / '$G' / 'streams' / 'WF_RUN'
                dst = out / 'copies' / mode / f'node-{node}'
                shutil.copytree(src, dst)
                index = dst / 'msgs/sched.db'
                raw = index.read_bytes()
                assert raw.hex() == '01000000000000000081841e0000000000', 'wrong original scheduling index'
                if mode == 'removed':
                    (out / f'node-{node}-original-sched.db').write_bytes(raw)
                    index.unlink()
                    mutation.append({'copy': str(index.relative_to(out)), 'operation': 'remove',
                                     'original_sha256': hashlib.sha256(raw).hexdigest()})
        (out / 'copy-mutations.json').write_text(json.dumps(mutation, indent=2) + '\n')
        # Go forbids overlays within GOMODCACHE. Compile an unchanged source copy
        # with only this additional diagnostic test, leaving the module cache intact.
        build = out / 'nats-source'
        shutil.copytree(module, build)
        (build / 'server').chmod(0o755)
        (build / 'server/workflow_copied_store_boundary_test.go').write_bytes(fixture.read_bytes())
        original_source = hashes(module)
        copied_source = hashes(build)
        copied_source.pop('server/workflow_copied_store_boundary_test.go')
        assert original_source == copied_source, 'copied module source differs'
        (out / 'module-inventory.json').write_text(json.dumps(original_source, indent=2) + '\n')
        env = dict(os.environ, GOWORK='off', GOMEMLIMIT='512MiB', GOMAXPROCS='2',
                   WF_COPIED_STORE_BOUNDARY_ROOT=str(out))
        cmd = ['go', 'test', '-p=1', '-json',
               './server', '-run', '^' + TEST + '$', '-count=1', '-timeout=3m']
        (out / 'command.json').write_text(json.dumps(cmd, indent=2) + '\n')
        with (out / 'events.jsonl').open('w') as stdout, (out / 'stderr.log').open('w') as stderr:
            result = subprocess.run(cmd, cwd=build, env=env, stdout=stdout, stderr=stderr, timeout=240)
        assert result.returncode == 0, 'actual pinned file-store experiment failed; retained evidence'
        events = [json.loads(line) for line in (out / 'events.jsonl').read_text().splitlines() if line.startswith('{')]
        assert not any(e['Action'] in ['skip', 'fail', 'build-fail'] for e in events)
        for name in [TEST] + [f'{TEST}/{mode}/node-{node}' for mode in ['intact','removed'] for node in range(3)]:
            selected = [e for e in events if e.get('Test') == name]
            assert sum(e['Action'] == 'run' for e in selected) == 1
            assert [e['Action'] for e in selected if e['Action'] in ['pass','fail','skip']] == ['pass']
        assert [e['Action'] for e in events if not e.get('Test') and e['Action'] in ['pass','fail','skip']] == ['pass']
        (out / 'result.json').write_text(json.dumps({'version': version, 'actual_copy_cases': 6,
            'original_physical_counts': [768, 141, 0], 'intact_schedule_counts': [0, 0, 0],
            'removed_schedule_counts': [768, 141, 0], 'server_processes': 0, 'publications': 0,
            'scope': 'Direct file-store recovery boundary; initial persistence inconsistency cause unconfirmed.'}, indent=2) + '\n')
    finally:
        after = hashes(original)
        (out / 'original-after.json').write_text(json.dumps(after, indent=2) + '\n')
        assert before == after, 'original campaign changed'
        assert source == {n: hashlib.sha256((module / n).read_bytes()).hexdigest() for n in names}, 'module source changed'
    print((out / 'result.json').read_text())


if __name__ == '__main__':
    main()
