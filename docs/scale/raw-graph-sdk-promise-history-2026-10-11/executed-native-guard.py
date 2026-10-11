import pathlib, re
log = pathlib.Path('docs/scale/raw-graph-sdk-promise-history-2026-10-11/native-sdk-race.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for test in ('TestNativeGraphContinuationSDKFlow', 'TestNativeGraphContinuationChildPromise', 'TestNativeGraphContinuationBufferedChildPromise'):
    for layout in ('R1', 'R3Domain'):
        assert f'--- PASS: {test}/{layout} ' in log
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=\d+ entries=\d+ terminals=[1-9]\d* pending=0', log)) == 6
retired = re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=\d+ entries=\d+ terminals=[1-9]\d* pending=0 retired_projection_only=([01])', log)
assert len(retired) == 6 and retired.count('1') == 4 and retired.count('0') == 2
