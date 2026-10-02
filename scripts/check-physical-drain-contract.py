#!/usr/bin/env python3
"""Verify physical replica drain with retained-store snapshots and compiled controls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'docs/scale/million-timer-terminal-2026-10-02/message-review'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    out = Path(args.root).resolve()
    out.mkdir(parents=True, exist_ok=False)
    snapshots = out / 'retained-snapshots'
    snapshots.mkdir()
    manifest = {m['path']: m for m in json.loads((FIXTURE / 'manifest.json').read_text())}
    with tarfile.open(FIXTURE / 'originals.tar.gz') as archive:
        for node in range(3):
            name = f'snapshot-14-node-{node}.json'
            data = archive.extractfile(name).read()
            assert hashlib.sha256(data).hexdigest() == manifest[name]['sha256']
            (snapshots / name).write_bytes(data)
    files = ('cmd/wf-timer-volume/main.go', 'cmd/wf-timer-volume/drain.go',
             'cmd/wf-timer-volume/physical_drain.go', 'cmd/wf-timer-volume/physical_drain_test.go',
             'cmd/wf-timer-volume/verify.go', 'cmd/wf-timer-volume/verify_test.go',
             'scripts/check-physical-drain-contract.py', '.github/workflows/physical-drain-contract.yml', 'go.mod', 'go.sum')
    hashes = {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in files}
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    (out / 'source.json').write_text(json.dumps({'head': head, 'files': hashes}, indent=2) + '\n')
    env = dict(os.environ, WF_PHYSICAL_DRAIN_SNAPSHOTS=str(snapshots))

    def run(label, pattern=None, overlay=None):
        command = ['go', 'test', '-race', '-p=1', '-json']
        if overlay: command += ['-overlay=' + str(overlay)]
        command += ['./cmd/wf-timer-volume', '-count=1', '-timeout=2m']
        if pattern: command += ['-run', pattern]
        with (out / (label + '.jsonl')).open('w') as stdout, (out / (label + '.stderr')).open('w') as stderr:
            done = subprocess.run(command, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, timeout=150)
        events = [json.loads(s) for s in (out / (label + '.jsonl')).read_text().splitlines() if s.startswith('{')]
        assert not any(e['Action'] == 'build-fail' or 'panic: test timed out' in e.get('Output', '') for e in events)
        return done.returncode, events

    def actual(events, test, action):
        selected = [e for e in events if e.get('Test') == test]
        assert sum(e['Action'] == 'run' for e in selected) == 1
        assert [e['Action'] for e in selected if e['Action'] in ('pass', 'fail', 'skip')] == [action], test

    code, events = run('positive')
    assert code == 0
    assert [e['Test'] for e in events if e['Action'] == 'skip'] == ['TestReceiptLedgerProcessKillHelper']
    for test in ('TestPhysicalDrainRejectsFalseEmptyReplicas', 'TestPhysicalDrainAgainstRetainedStoreSnapshots',
                 'TestPhysicalDrainRetainsPartialResponseAtDeadline', 'TestMillionReleaseVerifierCannotDowngradeLedgerEvidence',
                 'TestReceiptRecoveryAfterActualProcessKill'):
        actual(events, test, 'pass')
    controls = [('ignore-local-messages', 'cmd/wf-timer-volume/physical_drain.go',
                 '|| messages != 0', '|| messages > ^uint64(0)', 'TestPhysicalDrainAgainstRetainedStoreSnapshots', 'retained physical sources certified empty'),
                ('strip-release-proof', 'cmd/wf-timer-volume/verify.go',
                 'if !allowSmoke && (rep.LastDrainAudit == nil || rep.LastPhysicalDrainAudit == nil) {',
                 'if false {', 'TestMillionReleaseVerifierCannotDowngradeLedgerEvidence', 'stripped physical audit accepted')]
    for label, name, old, new, test, marker in controls:
        original = (ROOT / name).read_text()
        assert original.count(old) == 1
        mutant = out / (label + '.go')
        mutant.write_text(original.replace(old, new, 1))
        overlay = out / (label + '-overlay.json')
        overlay.write_text(json.dumps({'Replace': {str(ROOT / name): str(mutant)}}, indent=2) + '\n')
        code, events = run(label, '^' + test + '$', overlay)
        assert code == 1
        actual(events, test, 'fail')
        assert not any(e['Action'] == 'skip' for e in events)
        assert any(marker in e.get('Output', '') for e in events), 'unrelated failure'
    assert hashes == {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in files}
    result = {'head': head, 'actual_retained_snapshot_messages': [768, 141, 0],
              'actual_compiled_controls_detected': 2, 'physical_replicas_required': 3,
              'consumers_per_replica_required': 64, 'release_proof_cannot_be_stripped': True,
              'million_campaign_passed': False, 'full_release_gate': False}
    (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__': main()
