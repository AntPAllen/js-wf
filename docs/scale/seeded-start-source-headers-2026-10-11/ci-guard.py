from pathlib import Path
import re, sys
mode = sys.argv[1] if len(sys.argv) > 1 else 'race'
assert mode in ('normal', 'race')
log = Path(__file__).with_name(mode+'.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
assert '--- PASS: TestSeededGraphStartSourceHeadersReplay ' in log
for mode in ('default', 'explicit'):
    assert f'--- PASS: TestGraphStartHeaderFailureTracePersistence/{mode} ' in log
assert 'TIER1_SEEDS test=TestSeededGraphStartSourceHeadersReplay first=1 last=1000 completed=1000 requested=1000' in log
for stage in ('pending', 'bound'):
    for mode in ('valid', 'transport', 'duplicate_token', 'conflicting_hash', 'token_alias', 'empty_hash', 'missing_hash', 'input_ref_empty', 'parent_empty', 'parent_alias'):
        assert f'--- PASS: TestPinnedRegressionCorpus/graph_start_source_headers_{stage}_{mode}.json ' in log
print('1000 seed bodies, all20 pins and failure trace persistence passed')
