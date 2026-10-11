assert '--- PASS: TestRawGraphCheckpointSDKTimerHistory ' in log
for mode in ('absolute-position-and-deadline', 'cancel', 'await', 'timer-select', 'many-select', 'signal-then-cancel', 'carried-cancel', 'empty', 'missing-creation', 'wrong-name', 'wrong-deadline', 'wrong-domain', 'unconfirmed-cancel', 'cancel-after-fire', 'await-after-cancel', 'duplicate-cancel', 'live-checkpoint', 'omitted-cancel', 'fabricated-cancel', 'duplicate-cancel-id', 'bad-select-index', 'unselected-dead-timer'):
    assert f'--- PASS: TestRawGraphCheckpointSDKTimerHistory/{mode} ' in log
