import pathlib, re, sys
log = pathlib.Path(sys.argv[1]).read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
assert '--- PASS: TestSeededGraphProjectionCensusReplay ' in log
for mode in ('default', 'explicit'):
    assert f'--- PASS: TestGraphStartHeaderFailureTracePersistence/{mode} ' in log
assert 'TIER1_SEEDS test=TestSeededGraphProjectionCensusReplay first=1 last=1000 completed=1000 requested=1000' in log
for stage in ('live', 'retired'):
    for mode in ('healthy', 'orphan', 'missing', 'wrong_generation', 'stale_reader', 'create_drop', 'create_lost_ack'):
        assert f'--- PASS: TestPinnedRegressionCorpus/graph_projection_census_{stage}_{mode}.json ' in log
print('1000 bodies, 14 pins and failure persistence receipts verified')
