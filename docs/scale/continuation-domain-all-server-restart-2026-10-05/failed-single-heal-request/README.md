# Failed single-request post-restart domain observation

Clean506ab50 actual race SDKFAIL29.94s. Partial admission now records confirmed
state leaderwf-test-2, all three original library servers stopped, all three
new server IDs started, then zero healed-domain observations. The first account
API read exhausts the whole5s observation context; cut elapsed5.500761s. Strict
completed-fault count remains0 and fails. This identifies the rejected admission
stage; it does not establish a runtime or server defect. Original workload
recovers to the later fault-count assertion without promoting the fault gate.
Complete actual SDK/source/stores/admissions/logs and archive members/parts
retained/read back. Next test change retries only this idempotent read with250ms
attempts inside the unchanged shared5s observation budget, rejecting wrong-domain
success immediately. Original30s readiness/60s scenario budgets stay.
