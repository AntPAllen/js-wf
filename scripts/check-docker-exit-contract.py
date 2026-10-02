#!/usr/bin/env python3
"""Verify concurrent Docker exit bounds and reject a sequential observer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TESTS = ('TestDockerKillObservationSeparatesExitFromCleanup',
         'TestDockerKillObservationDoesNotWaitForKillReply',
         'TestDockerKillObservationRejectsFailedListing',
         'TestDockerKillObservationNative',
         'TestDockerKillObservationNativeDelayedReply')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--image', required=True, help='already-built pinned NATS image')
    args = parser.parse_args()
    out = Path(args.root).resolve()
    out.mkdir(parents=True, exist_ok=False)
    names = ('testcluster/docker_cluster.go', 'testcluster/docker_kill_observation_test.go',
             'scripts/check-clock-timer-cut.py', 'scripts/test_tier3_clock_timer_cut.py',
             'scripts/check-docker-exit-contract.py', '.github/workflows/docker-exit-contract.yml',
             '.github/workflows/tier3-mixed-journal.yml',
             'go.mod', 'go.sum')
    hashes = {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names}
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    image = subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', args.image], text=True).strip()
    (out / 'source.json').write_text(json.dumps({'head': head, 'files': hashes, 'image': image}, indent=2) + '\n')
    env = dict(os.environ, WF_DOCKER_EXIT_IMAGE=args.image)

    def run(label, pattern, overlay=None):
        cmd = ['go', 'test', '-race', '-p=1', '-json']
        if overlay:
            cmd += ['-overlay=' + str(overlay)]
        cmd += ['./testcluster', '-run', pattern, '-count=1', '-timeout=2m']
        path = out / (label + '.jsonl')
        with path.open('w') as stdout, (out / (label + '.stderr')).open('w') as stderr:
            result = subprocess.run(cmd, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, timeout=180)
        records = [json.loads(s) for s in path.read_text().splitlines() if s.startswith('{')]
        assert not any(e['Action'] in ('skip', 'build-fail') for e in records), 'skip/build failure'
        assert not any('panic: test timed out' in e.get('Output', '') for e in records), 'global timeout'
        return result.returncode, records

    def actual(records, name, action):
        selected = [e for e in records if e.get('Test') == name]
        assert sum(e['Action'] == 'run' for e in selected) == 1, name
        assert [e['Action'] for e in selected if e['Action'] in ('pass', 'fail', 'skip')] == [action], name

    status, positive = run('positive', '^TestDockerKillObservation')
    assert status == 0
    for name in TESTS:
        actual(positive, name, 'pass')
    for suffix in ('auto-remove-false', 'auto-remove-true'):
        actual(positive, 'TestDockerKillObservationNative/' + suffix, 'pass')
    assert [e['Action'] for e in positive if not e.get('Test') and e['Action'] in ('pass', 'fail')] == ['pass']
    with (out / 'verifier.log').open('w') as log:
        result = subprocess.run(['python3', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_tier3_clock_timer_cut.py'], cwd=ROOT, stdout=log, stderr=log, timeout=30)
    assert result.returncode == 0
    original = (ROOT / 'testcluster/docker_cluster.go').read_text()
    begin = original.index('func observeDockerKill(')
    end = original.index('\nfunc (c *DockerCluster) PauseNode', begin)
    body = original[begin:end]
    assert body.count('go func() {') == 1
    # The sole mutation waits for the kill reply before any state read.
    mutant = original[:begin] + body.replace('go func() {', 'func() {', 1) + original[end:]
    changed = out / 'sequential-observer.go'
    changed.write_text(mutant)
    overlay = out / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(ROOT / 'testcluster/docker_cluster.go'): str(changed)}}, indent=2) + '\n')
    status, negative = run('sequential', '^TestDockerKillObservationDoesNotWaitForKillReply$', overlay)
    assert status == 1
    actual(negative, TESTS[1], 'fail')
    assert [e['Action'] for e in negative if not e.get('Test') and e['Action'] in ('pass', 'fail')] == ['fail']
    assert any('kill reply prevented observing stopped server' in e.get('Output', '') for e in negative), 'wrong failure'
    assert hashes == {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names}, 'source changed'
    report = {'head': head, 'actual_positive_tests': len(TESTS), 'both_auto_remove_modes': True,
              'actual_delayed_docker_reply': True, 'sequential_source_control_detected': True,
              'strict_duration_gate_preserved': True, 'full_release_gate': False}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
