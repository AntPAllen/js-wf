assert '--- PASS: TestRawGraphEveryCheckpointHistory ' in log
for encoding in ('json', 'protobuf-v1'):
    for mode in ('historical-valid', 'historical-state', 'historical-anchor', 'historical-locals', 'historical-cursor', 'historical-timer', 'historical-metadata'):
        assert f'--- PASS: TestRawGraphEveryCheckpointHistory/{encoding}/{mode} ' in log
