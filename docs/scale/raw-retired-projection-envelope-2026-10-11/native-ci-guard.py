from pathlib import Path
import re
log = Path(__file__).with_name('native.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    for archive in ('false', 'true'):
        for buffered in ('false', 'true'):
            assert f'--- PASS: TestNativeGraphContinuationFailedChildPromise/{layout}/archive={archive}/buffered={buffered} ' in log
for archive in ('false', 'true'):
    for buffered in ('false', 'true'):
        assert log.count(f'GRAPH_FAILED_CHILD buffered={buffered} archive={archive} child_calls=1 preserved_error=planned_child_failure parent_result=43') == 2
assert log.count('RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=25 terminals=1 pending=0 retired_projection_only=1') == 8
assert log.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 8
print('all eight retired-child cases and sixteen frames passed')
