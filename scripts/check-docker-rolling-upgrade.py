#!/usr/bin/env python3
"""Qualify every-peer old-version Docker startup and retained-store upgrades."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestFiveDockerRollingUpgradePreservesEveryReplica'


def source_hashes():
    names = subprocess.check_output(['git', 'ls-files'], cwd=REPO, text=True).splitlines()
    return {name: hashlib.sha256((REPO/name).read_bytes()).hexdigest() for name in names
            if name.endswith('.go') or name in ('go.mod', 'go.sum',
                'scripts/check-docker-rolling-upgrade.py', '.github/workflows/docker-rolling-upgrade.yml')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    assert not root.exists() and not root.is_relative_to(REPO), 'new output root outside repository required'
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'clean committed source required'
    assert subprocess.check_output(['go', 'list', '-m', '-f', '{{.Version}}',
        'github.com/nats-io/nats-server/v2'], cwd=REPO, text=True).strip() == 'v2.15.0', 'contract pins current NATS2.15.0'
    root.mkdir(parents=True)
    before = source_hashes()
    (root/'source-before.json').write_text(json.dumps(dict(
        revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
        files=before), indent=2)+'\n')
    source = REPO/'testcluster/docker_cluster.go'
    text = source.read_text()
    needle = 'c.oldNodes[node] = allOld || node == 0'
    assert text.count(needle) == 1, 'precise original single-old-peer control required'
    mutated = root/'single-old-peer-control.go'
    mutated.write_text(text.replace(needle, 'c.oldNodes[node] = node == 0'))
    overlay = root/'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(source): str(mutated)}})+'\n')
    try:
        for mode in ('positive', 'negative'):
            cmd = ['go', 'test', '-p=1', '-race', '-json']
            if mode == 'negative': cmd += ['-overlay='+str(overlay)]
            cmd += ['./testcluster', '-run', '^'+TEST+'$', '-count=1', '-timeout=18m']
            (root/(mode+'-command.json')).write_text(json.dumps(cmd, indent=2)+'\n')
            env = dict(os.environ, GOMEMLIMIT='512MiB', GOMAXPROCS='2', WF_DOCKER_ROLLING_UPGRADE='1',
                       WF_DOCKER_UPGRADE_ARTIFACT_ROOT=str(root/mode))
            with (root/(mode+'-events.jsonl')).open('w') as out, (root/(mode+'-stderr.log')).open('w') as err:
                result = subprocess.run(cmd, cwd=REPO, env=env, stdout=out, stderr=err)
            events = [json.loads(line) for line in (root/(mode+'-events.jsonl')).read_text().splitlines()]
            assert not any(e['Action'] in ('build-fail', 'skip') for e in events), 'build/skip is not execution proof'
            named = [e['Action'] for e in events if e.get('Test') == TEST and e['Action'] in ('pass', 'fail', 'skip')]
            package = [e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass', 'fail')]
            if mode == 'positive':
                assert result.returncode == 0 and named == ['pass'] and package == ['pass'], 'actual all-peer contract failed'
                proof = json.loads((root/mode/'result.json').read_text())
                assert proof['order'] == [2, 0, 4, 1, 3] and proof['retained_messages'] == 37 and proof['replicas'] == 5
            else:
                output = ''.join(e.get('Output', '') for e in events)
                assert result.returncode != 0 and named == ['fail'] and package == ['fail']
                assert output.count('stage=initial node1 version=2.15.0 want=2.11.17') == 1, 'unrelated failure is not semantic detection'
                assert 'panic: test timed out' not in output, 'global timeout is not semantic detection'
        (root/'result.json').write_text(json.dumps(dict(actual_race_positive=True,
            actual_single_old_peer_control_detected=True, upgrades=5, physical_replicas=5,
            retained_messages=37, scope='Docker upgrade fixture contract, not mixed workload or full R5 matrix.'), indent=2)+'\n')
    finally:
        after = source_hashes()
        (root/'source-after.json').write_text(json.dumps(after, indent=2)+'\n')
        assert before == after, 'repository source changed during qualification'


if __name__ == '__main__':
    main()
