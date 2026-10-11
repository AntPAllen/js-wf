assert '--- PASS: TestRawGraphRejectedLimitBoundary ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('request', 'attempt', 'signal', 'suspension', 'overlap', 'completed', 'both', 'request-null', 'request-empty', 'request-unknown', 'request-duplicate', 'entry-kind', 'entry-null', 'attempt-gap', 'attempt-empty-error', 'attempt-alias', 'attempt-duplicate', 'signal-zero', 'signal-name', 'signal-alias', 'suspension-unresolved', 'suspension-empty', 'suspension-unknown', 'wrong-error'):
        assert f'--- PASS: TestRawGraphRejectedLimitBoundary/{encoding}/{mode} ' in log
assert '--- PASS: TestRawGraphRejectedLimitPrefix ' in log
for mode in ('attempt-next', 'attempt-reuse', 'attempt-skip', 'signal-next', 'signal-reuse', 'signal-backwards'):
    assert f'--- PASS: TestRawGraphRejectedLimitPrefix/{mode} ' in log
assert '--- PASS: TestRawGraphRejectedLimitContinuation ' in log
for mode in ('checkpoint', 'wrong-stage', 'not-checkpoint', 'failed-frame'):
    assert f'--- PASS: TestRawGraphRejectedLimitContinuation/{mode} ' in log
