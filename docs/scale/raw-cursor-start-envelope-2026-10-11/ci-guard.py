from pathlib import Path
log = Path(__file__).with_name('race.log').read_text()
assert '--- PASS: TestRawGraphCursorEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for archive in ('false', 'true'):
        for control in ('valid', 'whitespace', 'unknown', 'alias', 'duplicate-schema', 'duplicate-invocation', 'escaped-count', 'count-fraction', 'invocation-negative'):
            assert f'--- PASS: TestRawGraphCursorEnvelope/{encoding}/archive={archive}/{control} ' in log
assert '--- PASS: TestRawGraphStartEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for control in ('valid', 'cursor-unknown', 'cursor-alias', 'cursor-duplicate', 'cursor-request-unknown', 'cursor-request-alias', 'cursor-request-duplicate', 'start-wire-unknown', 'start-wire-alias', 'start-wire-duplicate', 'start-wire-escaped', 'start-wire-request-unknown', 'start-wire-request-alias', 'start-wire-request-duplicate', 'start-parent-valid', 'start-parent-type', 'start-parent-id', 'start-parent-generation', 'start-parent-signal'):
        assert f'--- PASS: TestRawGraphStartEnvelope/{encoding}/{control} ' in log
print('all 74 cursor/start controls passed')
