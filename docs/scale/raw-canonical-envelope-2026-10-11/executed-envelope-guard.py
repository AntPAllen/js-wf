assert '--- PASS: TestRawGraphCanonicalEnvelope ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('outer-valid', 'outer-whitespace', 'outer-unknown', 'outer-alias', 'outer-duplicate', 'outer-escaped-duplicate', 'outer-generation', 'outer-sequence', 'outer-hash'):
        assert f'--- PASS: TestRawGraphCanonicalEnvelope/{encoding}/{mode} ' in log
