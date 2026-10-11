import pathlib, re
log = pathlib.Path('docs/scale/native-sdk-timer-history-2026-10-11/native-race.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    for archive in ('false', 'true'):
        assert f'--- PASS: TestNativeGraphContinuationTimerHistory/{layout}/archive={archive} ' in log
assert len(re.findall(r'SDK_TIMER_HISTORY stage=next cancelled=\[4\] archive=(?:true|false)', log)) == 4
assert len(re.findall(r'SDK_TIMER_HISTORY stage=finish cancelled=\[4,14\] archive=(?:true|false)', log)) == 4
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=\d+ terminals=1 pending=0 retired_projection_only=0', log)) == 4
