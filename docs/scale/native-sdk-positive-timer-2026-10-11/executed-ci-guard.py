import pathlib, re
log = pathlib.Path('docs/scale/native-sdk-positive-timer-2026-10-11/native-race.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    assert f'--- PASS: TestNativeGraphContinuationScheduledTimerFlow/{layout} ' in log
assert log.count('SDK_POSITIVE_TIMER suspended=1 handoffs=2 start=1 schedules=3 initial=2 next=1 finish=1 effects=2') == 2
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=\d+ terminals=1 pending=0 retired_projection_only=0', log)) == 2
