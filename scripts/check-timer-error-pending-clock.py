#!/usr/bin/env python3
"""Qualify the timer-error pending-clock characterization and its local-NAK control."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestWorkerTimerErrorPendingClockCharacterization'
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
a = p.parse_args()
root = a.root.resolve()
assert not root.exists() and not root.is_relative_to(REPO)
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO)
names = subprocess.check_output(['git', 'ls-files'], cwd=REPO, text=True).splitlines()
def inventory():
    return {n: hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in names
            if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/regressions/')}
root.mkdir(parents=True)
before = inventory()
(root/'source-before.json').write_text(json.dumps(dict(revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(), files=before), indent=2)+'\n')
source = REPO/'sim/dispatch_transport.go'
original = source.read_text()
needle = 'if fault == "drop_before_commit_success" {\n\t\t\t\treturn nil\n\t\t\t}'
assert original.count(needle) == 1
(root/'visible-nak-control.go.txt').write_text(original.replace(needle, 'if fault == "drop_before_commit_success" {\n\t\t\t\treturn ErrTransportLost\n\t\t\t}'))
overlay = root/'overlay.json'
overlay.write_text(json.dumps({'Replace': {str(source): str(root/'visible-nak-control.go.txt')}})+'\n')
try:
    for mode in ('positive', 'negative'):
        expression = '^Test(WorkerTimerErrorPendingClockCharacterization|PinnedRegressionCorpus)$' if mode == 'positive' else '^'+TEST+'$/unapplied_nak_true$'
        command = ['go', 'test', '-p=1', '-race', '-json']
        if mode == 'negative': command += ['-overlay='+str(overlay)]
        command += ['./sim', '-run', expression, '-count=1', '-timeout=6m']
        (root/(mode+'-command.json')).write_text(json.dumps(command, indent=2)+'\n')
        env = dict(os.environ, GOMEMLIMIT='512MiB', GOMAXPROCS='2', TIMER_ERROR_TRACE_ROOT=str(root/'traces'))
        with (root/(mode+'-events.jsonl')).open('w') as stdout, (root/(mode+'-stderr.log')).open('w') as stderr:
            result = subprocess.run(command, cwd=REPO, env=env, stdout=stdout, stderr=stderr)
        events = [json.loads(line) for line in (root/(mode+'-events.jsonl')).read_text().splitlines()]
        assert not any(e['Action'] in ('skip', 'build-fail') for e in events)
        output = ''.join(e.get('Output', '') for e in events)
        assert 'panic: test timed out' not in output
        def actions(test): return [e['Action'] for e in events if e.get('Test') == test and e['Action'] in ('pass', 'fail')]
        package = [e['Action'] for e in events if not e.get('Test') and e['Action'] in ('pass', 'fail')]
        if mode == 'positive':
            assert result.returncode == 0 and actions(TEST) == ['pass'] and package == ['pass']
            for lost in ('false', 'true'): assert actions(TEST+'/unapplied_nak_'+lost) == ['pass']
            assert actions('TestPinnedRegressionCorpus') == ['pass']
            assert output.count('exact_replay=true production_timer=true') == 2
        else:
            assert result.returncode == 1 and actions(TEST) == ['fail'] and package == ['fail']
            assert actions(TEST+'/unapplied_nak_true') == ['fail']
            assert 'local NAK acceptance was not successful:' in output
    (root/'result.json').write_text(json.dumps(dict(actual_race_characterization_pass=True, exact_replay_cases=2,
        actual_local_nak_control_detected=True, production_worker_changed=False,
        qualifies_full_seed_gate=False, confirms_seed55_server_cause=False), indent=2)+'\n')
finally:
    after = inventory()
    (root/'source-after.json').write_text(json.dumps(after, indent=2)+'\n')
    assert after == before
