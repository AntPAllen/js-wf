import pathlib, re
log = pathlib.Path('docs/scale/native-sdk-cancelled-timer-wakeup-2026-10-11/native-race.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    assert f'--- PASS: TestNativeGraphContinuationCancelledTimerWakeup/{layout} ' in log
for stage, step in (('next', 4), ('finish', 4), ('finish', 14)):
    assert log.count(f'SDK_CANCELLED_TIMER_WAKEUP stage={stage} step={step} no_op=true immutable_history=true unchanged_effects=true archive=true') == 2
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=\d+ terminals=1 pending=0 retired_projection_only=0', log)) == 2
