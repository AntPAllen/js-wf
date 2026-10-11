from pathlib import Path
log = Path(__file__).with_name('race.log').read_text()
assert '--- PASS: TestRawGraphRetiredProjectionEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for control in ('inline', 'opaque', 'empty', 'whitespace', 'failed', 'external', 'limit-request', 'limit-entry', 'unknown', 'alias', 'duplicate-generation', 'escaped-generation', 'duplicate-result', 'duplicate-error', 'unknown-limit-entry', 'alias-limit-entry', 'duplicate-limit-entry', 'null', 'array', 'base64', 'generation-fraction', 'generation-negative', 'failed-inline', 'failed-pointer', 'hash-only', 'pointer-only', 'bad-hash', 'mixed-result', 'completed-limit', 'both-limits', 'wrong-limit-error'):
        assert f'--- PASS: TestRawGraphRetiredProjectionEnvelope/{encoding}/{control} ' in log
print('all 62 retired projection controls passed')
