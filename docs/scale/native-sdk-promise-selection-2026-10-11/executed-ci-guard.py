import pathlib, re
log = pathlib.Path('raw-sdk-promise-selection.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    for archive in ('false', 'true'):
        for failed in ('false', 'true'):
            assert f'--- PASS: TestNativeGraphContinuationPromiseSelectionPriority/{layout}/archive={archive}/failed={failed} ' in log
for stage in (1, 2):
    for failed in ('false', 'true'):
        assert log.count(f'SDK_PROMISE_PRIORITY stage={stage} case=1 duplicate_ready_case=2 timer_loser_reused=true failed={failed}') == 4
assert len(re.findall(r'SDK_PROMISE_CACHE first_signal=[1-9]\d* cached_signal=0 selections=2 child_calls=1 failed=(?:false|true) archive=(?:false|true)', log)) == 8
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=35 terminals=1 pending=0 retired_projection_only=1', log)) == 8
assert log.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 8
