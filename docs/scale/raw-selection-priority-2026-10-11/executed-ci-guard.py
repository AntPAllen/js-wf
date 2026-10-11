assert '--- PASS: TestRawGraphSelectionPriority ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('first', 'reordered', 'earlier-absent', 'skipped-ready', 'invalid-unselected-kind', 'invalid-unselected-name'):
        assert f'--- PASS: TestRawGraphSelectionPriority/{encoding}/{mode} ' in log
assert '--- PASS: TestRawGraphSelectionReadiness ' in log
for mode in ('signal-first', 'signal-skipped', 'signal-used', 'promise-buffered-skipped', 'promise-cached-skipped', 'promise-ambiguous', 'timer-zero-skipped', 'timer-positive-unknown', 'timer-cancelled', 'timer-fired', 'timer-signal-buffered', 'timer-signal-used', 'timer-signal-absent', 'timer-before-signal', 'invalid-child'):
    assert f'--- PASS: TestRawGraphSelectionReadiness/{mode} ' in log
