from pathlib import Path
log = Path(__file__).with_name('race.log').read_text()
assert '--- PASS: TestRawGraphCursorState ' in log
for encoding in ('json', 'protobuf-v1'):
    for archive in ('false', 'true'):
        for retired in ('false', 'true'):
            for control in ('valid', 'root-version', 'previous-generation', 'signal-repair', 'unknown-forest', 'retired-token', 'unknown-kind', 'singleton-terminal'):
                assert f'--- PASS: TestRawGraphCursorState/{encoding}/archive={archive}/retired={retired}/{control} ' in log
assert '--- PASS: TestRawGraphCursorVersionCapabilities ' in log
for version in ('v1', 'v2', 'v4', 'v5', 'v6'):
    for control in ('valid', 'signal-hint', 'checkpoint', 'archive', 'pending-signal', 'purging', 'input-missing'):
        assert f'--- PASS: TestRawGraphCursorVersionCapabilities/{version}/{control} ' in log
print('all64 physical and35 capability controls passed')
