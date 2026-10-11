assert '--- PASS: TestRawGraphSDKStepEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('ordinary', 'opaque-field-names', 'opaque-duplicates', 'opaque-null', 'request-null', 'request-array', 'request-unknown', 'request-alias', 'request-duplicate', 'request-escaped-duplicate', 'request-time-type', 'request-step-negative', 'case-unknown', 'case-alias', 'case-duplicate', 'case-shape', 'completion-null', 'completion-array', 'completion-unknown', 'completion-alias', 'completion-duplicate', 'completion-sequence-negative', 'completion-case-fraction', 'completion-cancel-type', 'metadata-ref-null', 'metadata-ref-object', 'metadata-hash-null', 'metadata-hash-number'):
        assert f'--- PASS: TestRawGraphSDKStepEnvelope/{encoding}/{mode} ' in log
