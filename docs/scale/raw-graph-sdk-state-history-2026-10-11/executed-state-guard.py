assert '--- PASS: TestRawGraphCheckpointSDKStateHistory ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('history-valid', 'history-external-valid', 'history-read-absent-valid', 'history-write-error-valid', 'history-null-valid'):
        assert f'--- PASS: TestRawGraphCheckpointSDKStateHistory/{encoding}/{mode} ' in log
for mode in ('history-frame-value', 'history-frame-missing', 'history-frame-extra', 'history-write-hash', 'history-write-missing', 'history-write-error', 'history-read-fabricated', 'history-result-null', 'history-result-unknown', 'history-external-unowned', 'history-external-wrong'):
    assert f'--- PASS: TestRawGraphCheckpointSDKStateHistory/{mode} ' in log
