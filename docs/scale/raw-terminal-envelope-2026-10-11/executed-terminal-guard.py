assert '--- PASS: TestRawGraphTerminalEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('ordinary', 'opaque-result', 'failed', 'limit-request', 'limit-entry', 'unknown', 'alias', 'duplicate-invocation', 'escaped-duplicate', 'duplicate-result', 'duplicate-error', 'duplicate-limit-request', 'duplicate-limit-entry', 'limit-entry-unknown', 'limit-entry-alias', 'limit-entry-duplicate'):
        assert f'--- PASS: TestRawGraphTerminalEnvelope/{encoding}/{mode} ' in log
