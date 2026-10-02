#!/usr/bin/env python3
"""Execute and verify combined continuation cap, worker kill and server restart."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TEST = 'TestContinuationLimitAfterWorkerKillAndClusterRestart'
CUTS = ('after_signal', 'after_completion', 'after_failed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--budget', type=int, choices=(16, 20, 100000), default=16)
    args = parser.parse_args()
    out = Path(args.root).resolve()
    out.mkdir(parents=True, exist_ok=False)
    names = ('worker/worker.go', 'worker/continuation.go', 'worker/continuation_limit_test.go',
             'worker/continuation_limit_kill_test.go', 'scripts/check-continuation-limit-kill.py',
             '.github/workflows/continuation-limit-combined.yml', 'go.mod', 'go.sum')
    hashes = {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names}
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    (out / 'source.json').write_text(json.dumps({'head': head, 'files': hashes}, indent=2) + '\n')
    env = dict(os.environ, WF_CONTINUATION_LIMIT_KILL='1', WF_CONTINUATION_LIMIT_KILL_BUDGET=str(args.budget))

    def run(label, pattern, overlay=None):
        rows = out / label
        cmd = ['go', 'test', '-race', '-p=1', '-json']
        if overlay: cmd += ['-overlay=' + str(overlay)]
        cmd += ['./worker', '-run', pattern, '-count=1', '-timeout=' + ('80m' if args.budget == 100000 else '6m')]
        log = out / (label + '.jsonl')
        with log.open('w') as stdout, (out / (label + '.stderr')).open('w') as stderr:
            result = subprocess.run(cmd, cwd=ROOT, env=dict(env, LIMIT_KILL_ARTIFACT_ROOT=str(rows)), stdout=stdout, stderr=stderr, timeout=4860 if args.budget == 100000 else 420)
        records = [json.loads(s) for s in log.read_text().splitlines() if s.startswith('{')]
        assert not any(e['Action'] in ('skip', 'build-fail') or 'panic: test timed out' in e.get('Output', '') for e in records), 'skip/build/global timeout'
        return result.returncode, records, rows

    def actual(events, name, action):
        selected = [e for e in events if e.get('Test') == name]
        assert sum(e['Action'] == 'run' for e in selected) == 1, name
        assert [e['Action'] for e in selected if e['Action'] in ('pass', 'fail', 'skip')] == [action], name

    status, positive, rows = run('positive', '^' + TEST + '$')
    assert status == 0
    actual(positive, TEST, 'pass')
    for index, cut in enumerate(CUTS):
        actual(positive, TEST + '/' + cut, 'pass')
        root = rows / cut
        journal = json.loads((root / 'journal.json').read_text())
        prefix = json.loads((root / 'prefix.json').read_text())
        lease = json.loads((root / 'cut-lease.json').read_text())
        assert len(journal) == args.budget and len(prefix) == args.budget - 2 + index
        assert journal[:len(prefix)] == prefix and [r['index'] for r in journal] == list(range(args.budget))
        assert journal[-1]['kind'] == 'Failed' and journal[-1]['payload']['error'] == 'journal exceeds 100000 entries'
        assert journal[-1]['payload']['limit_request']['name'] == 'must_not_run'
        assert prefix[-1]['kind'] == ('SignalConsumed', 'StepCompleted', 'Failed')[index]
        if index < 2: assert journal[-1]['epoch'] > lease['epoch']
        assert 'effect\n' not in (root / 'child.handlers').read_text()
        assert 'effect\n' not in (root / 'child.offline').read_text()
        marker = 'CONTINUATION_LIMIT_KILL cut=' + cut + ' budget=' + str(args.budget)
        output = ''.join(e.get('Output', '') for e in positive if e.get('Test') == TEST + '/' + cut)
        assert marker in output and 'worker_sigkill=true all_server_restart=true lease_ttl=12s initial_lease_held=true' in output and 'effects=0 archive_reads=0' in output
    original = (ROOT / 'worker/worker.go').read_text()
    replacements = [('if nextIndex() >= w.maxEntries {', 'if uint64(len(records)) >= w.maxEntries {'),
                    ('nextIndex() >= w.maxEntries-1', 'uint64(len(records)) >= w.maxEntries-1'),
                    ('nextIndex() >= w.maxEntries-2', 'uint64(len(records)) >= w.maxEntries-2')]
    mutant = original
    for old, new in replacements:
        assert mutant.count(old) == 1
        mutant = mutant.replace(old, new, 1)
    changed = out / 'suffix-budget.go'
    changed.write_text(mutant)
    overlay = out / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(ROOT / 'worker/worker.go'): str(changed)}}, indent=2) + '\n')
    status, negative, rows = run('suffix-budget', '^' + TEST + '/after_signal$', overlay)
    assert status == 1
    actual(negative, TEST + '/after_signal', 'fail')
    assert any('limit outcome changed: <nil>' in e.get('Output', '') for e in negative), 'wrong detection'
    raw = json.loads((rows / 'after_signal/journal.json').read_text())
    assert len(raw) > args.budget and raw[-1]['kind'] == 'Completed'
    assert (rows / 'after_signal/child.handlers').read_text().count('effect\n') == 1
    assert hashes == {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names}, 'source changed'
    report = {'head': head, 'budget': args.budget, 'actual_combined_cuts': 3, 'actual_worker_sigkills': 3,
              'all_three_server_restarts_per_cut': True, 'production_lease_ttl_seconds': 12,
              'actual_suffix_budget_control_detected': True, 'production_cap': args.budget == 100000,
              'full_release_gate': False}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()
