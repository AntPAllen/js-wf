import hashlib, json, pathlib
base = pathlib.Path(__file__).resolve().parent
expected = {
    'normal.jsonl': {'TestGraphOperatorRejectsPartialOrLegacyOnlySelection', 'TestNativeCanonicalGraphOperatorCommands'},
    'legacy-normal.jsonl': {'TestOperatorCommands', 'TestOperatorCommandsInJetStreamDomain'},
    'race.jsonl': {'TestGraphOperatorRejectsPartialOrLegacyOnlySelection', 'TestNativeCanonicalGraphOperatorCommands', 'TestOperatorCommands', 'TestOperatorCommandsInJetStreamDomain'},
}
review = {}
for name, tests in expected.items():
    data = (base / name).read_bytes()
    rows = [json.loads(line) for line in data.splitlines()]
    assert not any(r['Action'] == 'fail' or 'WARNING: DATA RACE' in r.get('Output', '') for r in rows), name
    passed = {r['Test'] for r in rows if r['Action'] == 'pass' and 'Test' in r}
    assert {t for t in passed if '/' not in t} == tests, (name, passed)
    terminal = [r for r in rows if 'Test' not in r and r['Action'] in ('pass', 'fail')]
    assert len(terminal) == 1 and terminal[0]['Action'] == 'pass', name
    if 'TestNativeCanonicalGraphOperatorCommands' in tests:
        for case in ('R1', 'R3Domain'):
            assert 'TestNativeCanonicalGraphOperatorCommands/' + case in passed
        diagnostics = [r.get('Output','') for r in rows if 'canonical CLI large Start/Signal/result' in r.get('Output','')]
        assert len(diagnostics) == 2 and all('wrong=0' in s for s in diagnostics), name
    review[name] = {'sha256': hashlib.sha256(data).hexdigest(), 'elapsed': terminal[0]['Elapsed'], 'top_level_pass': sorted(tests)}
review['scope'] = 'Development component checks; no frozen full-suite or extended qualification claim.'
(base / 'review.json').write_text(json.dumps(review, indent=2) + '\n')
print(json.dumps(review))
