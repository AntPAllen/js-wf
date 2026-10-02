#!/usr/bin/env python3
"""Run and verify production timer deletion contracts and cause-loss controls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MODEL = "TestSeededNativeTimerDeleteReplies"
REAL = "TestNativeTimerDeleteReplyClassification"


def events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]


def actual(records, name, result):
    selected = [e for e in records if e.get('Test') == name]
    assert sum(e['Action'] == 'run' for e in selected) == 1, name
    assert [e['Action'] for e in selected if e['Action'] in ('pass', 'fail', 'skip')] == [result], name
    assert not any(e['Action'] == 'skip' for e in records), 'unexpected skip'
    assert not any('panic: test timed out' in e.get('Output', '') for e in records), 'timeout'
    assert not any(e['Action'] == 'build-fail' for e in records), 'build failure'


def run(out, label, packages, pattern, env, overlay=None, failure=False):
    command = ['go', 'test', '-race', '-p=1', '-json']
    if overlay:
        command += ['-overlay=' + str(overlay)]
    command += packages + ['-run', pattern, '-count=1', '-timeout=3m']
    path = out / (label + '.jsonl')
    with path.open('w') as stdout, (out / (label + '.stderr')).open('w') as stderr:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, timeout=240)
    assert result.returncode == (1 if failure else 0), (label, result.returncode)
    records = events(path)
    expected = 'fail' if failure else 'pass'
    for package in packages:
        endings = [e['Action'] for e in records if e.get('Package') == 'js-wf/' + package.removeprefix('./') and not e.get('Test') and e['Action'] in ('pass', 'fail', 'skip')]
        assert endings == [expected], (label, endings)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    out = Path(args.root).resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    files = sorted(set(ROOT.rglob('*.go')) | {ROOT / 'go.mod', ROOT / 'go.sum', Path(__file__).resolve(), ROOT / '.github/workflows/native-delete-contract.yml'})
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (out / 'source.json').write_text(json.dumps({'head': source, 'files': hashes}, indent=2) + '\n')
    env = dict(os.environ, SIM_SEEDS='1000', SIM_COVERAGE_SUMMARY='1')
    for key in ('SIM_NATIVE_DELETE_OUT', 'FAULT_SEED', 'FAULT_TRACE'):
        env.pop(key, None)
    positive = run(out, 'positive', ['./sim', './integration', './reconcile'], '^Test(SeededNativeTimerDeleteReplies|NativeTimerDeleteReplyClassification|NativeTimerRetirementAfterTerminalJournal|FallbackTimer.*)$', env)
    for name in (MODEL, REAL, 'TestNativeTimerRetirementAfterTerminalJournal', 'TestFallbackTimerSurvivesPollerDowntime', 'TestFallbackTimerScanRetriesCommittedWakeupBeforeDelete', 'TestFallbackTimerScanRemovesRetiredGenerations'):
        actual(positive, name, 'pass')
    marker = 'TIER1_SEEDS test=' + MODEL + ' first=1 last=1000 completed=1000 requested=1000'
    assert sum(marker in e.get('Output', '') for e in positive) == 1, 'seed range missing'
    real_cases = [e['Test'] for e in positive if e['Action'] == 'pass' and e.get('Test', '').startswith(REAL + '/') and e['Test'].count('/') == 2]
    assert len(set(real_cases)) == 14, 'real reply cases missing'
    pinned = run(out, 'pinned', ['./sim'], '^TestPinnedRegressionCorpus$', env)
    actual(pinned, 'TestPinnedRegressionCorpus', 'pass')
    pins = {p.name for p in (ROOT / 'sim/testdata/regressions').glob('*.json')}
    observed = {e['Test'].split('/', 1)[1] for e in pinned if e['Action'] == 'pass' and e.get('Test', '').startswith('TestPinnedRegressionCorpus/')}
    assert observed == pins, 'pin inventory differs'
    # Preserve the old SDK's cause-flattening semantics. Do not depend on its
    # error string formatting: acceptance requires actual production-loop failure.
    original = (ROOT / 'internal/natsutil/delete.go').read_text()
    start = original.index('func NormalizeMessageDeleteError(')
    end = original.index('\n// DeleteStreamMessage', start)
    mutant = original[:start] + '''func NormalizeMessageDeleteError(err error) error {
    var api *nats.APIError
    if errors.As(err, &api) {
        typed := &jetstream.APIError{Code: api.Code, ErrorCode: jetstream.ErrorCode(api.ErrorCode), Description: api.Description}
        return fmt.Errorf("%w: %s", jetstream.ErrMsgDeleteUnsuccessful, typed.Error())
    }
    return err
}
''' + original[end:]
    mutant = mutant.replace('"errors"', '"errors"\n    "fmt"', 1)
    changed = out / 'flattened-delete.go'
    changed.write_text(mutant)
    overlay = out / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(ROOT / 'internal/natsutil/delete.go'): str(changed)}}, indent=2) + '\n')
    negatives = {}
    for label, seed, mode, pin in [('absent', 3, 'already_absent', 'native-delete-already-absent.json'), ('unavailable', 18, 'unavailable', 'native-delete-unavailable.json')]:
        trace = out / (label + '-failure.json')
        records = run(out, label, ['./sim'], '^' + MODEL + '$', dict(env, SIM_NATIVE_DELETE_OUT=str(trace), FAULT_SEED=str(seed)), overlay, True)
        actual(records, MODEL, 'fail')
        assert any('retryable/idempotent native deletion killed production loop' in e.get('Output', '') for e in records), label
        data = json.loads(trace.read_text())
        assert data['seed'] == seed and data['decisions'][0]['chosen'] == mode, label
        # Separate processes export the intact trace and reproduce the checked-in pin.
        intact = out / (label + '-intact.json')
        exported = run(out, label + '-export', ['./sim'], '^' + MODEL + '$', dict(env, SIM_NATIVE_DELETE_OUT=str(intact), FAULT_SEED=str(seed)))
        actual(exported, MODEL, 'pass')
        assert intact.read_bytes() == (ROOT / 'sim/testdata/regressions' / pin).read_bytes(), 'cross-process pin mismatch'
        negatives[label] = {'seed': seed, 'mode': mode, 'actual_failure': True}
    negative_real = run(out, 'real-flattened', ['./integration'], '^' + REAL + '$', env, overlay, True)
    actual(negative_real, REAL, 'fail')
    for marker in ('delete reply lost typed API cause', 'already absent delete killed retirement'):
        assert any(marker in e.get('Output', '') for e in negative_real), marker
    assert hashes == {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}, 'source changed during run'
    report = {'head': source, 'seeds': 1000, 'real_reply_cases': 14, 'pins': len(pins), 'negative_controls': negatives, 'real_cause_loss_detected': True, 'full_release_gate': False}
    (out / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
