from pathlib import Path
import re
log = Path(__file__).with_name('native.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for test in ('TestNativeGraphContinuationChildPromise', 'TestNativeGraphContinuationBufferedChildPromise'):
    for layout in ('R1', 'R3Domain'):
        assert f'--- PASS: {test}/{layout} ' in log
assert log.count('RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=25 terminals=1 pending=0 retired_projection_only=1') == 4
assert log.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 4
print('all four native child cases and eight parent frames passed')
