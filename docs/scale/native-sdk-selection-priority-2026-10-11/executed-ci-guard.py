import pathlib, re
log = pathlib.Path('raw-sdk-selection-priority.log').read_text()
assert not re.search(r'^--- SKIP:', log, re.M)
for layout in ('R1', 'R3Domain'):
    for archive in ('false', 'true'):
        assert f'--- PASS: TestNativeGraphContinuationSelectionPriority/{layout}/archive={archive} ' in log
assert log.count('SDK_SELECTION_PRIORITY stage=next signal_over_timer=true timer_case=1 ready_timer_loser_reused=true') == 4
assert log.count('SDK_SELECTION_PRIORITY stage=finish signal_case=1 duplicate_ready_case=2') == 4
assert len(re.findall(r'RAW_SDK_CHECKPOINT_AUDIT journals=1 entries=\d+ terminals=1 pending=0 retired_projection_only=0', log)) == 4
assert log.count('RAW_SDK_CHECKPOINT_HISTORY checkpoints=2') == 4
