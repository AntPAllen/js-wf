# Full400k callback candidate capacity preparation

Set both WF_AUDIT_CAPACITY_PLAIN_COMPARISON=1 and
WF_AUDIT_CAPACITY_CALLBACK_COMPARISON=1 to add an explicit callback/concurrent
full audit between compact/concurrent and SDK/recheck. Every attempt retains20s
and the full400k invocations /4.8M journal entries /400k terminalstates. The callback
candidate must produce the exact complete report within20s or the test fails.
All baseline verdicts remain recorded, including existing compact failures.

The prepared producer uses the same four-core/GOGC200/2GiB configuration as the
previous failed profile. Only archive-verified fresh disposable copies are opened;
the original retained R5 dataset is checked before/after and stays closed.
Selected Git/actualSDK/server/module/mount/closure identities are captured.

Compilation/opt-in skip passes. Native capacity execution is pending; legacy
correctness review and verified disk headroom must complete before launch.
No default adoption or24h qualification claimed.
