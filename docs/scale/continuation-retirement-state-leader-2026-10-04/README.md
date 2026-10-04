# Continuation retirement, generation reuse and state-leader restart

`TestContinuationRetirementReuseWithManifestLossAndStateLeaderRestart` extends
the existing shared-blob retirement/GC/generation-reuse fixture. At fresh manifest
publication, it confirms the actual `KV_WF_STATE` leader, shuts it down through
the NATS server library, confirms it stopped and restarts its original store.
The port then returns the existing controlled pre-commit `ErrUnknown`. SDK
connections reconnect while the existing worker publication budgets remain.

The focused race run passed in22.05s (package23.101s): leaderwf-test-1 restarted,
old generation1/fresh3, two retired objects reclaimed, three effects, two terminal
invocations and a retained shared blob. Fresh initial handler executed twice for
manifest repair; exactly one response loss and one actual leader restart are
required. Old-frame rejection precedes user effects, every peer returns the new
result and the raw integrity check reports both terminals. Survivor and fresh
frame references remain reachable after a second quiescent collection.

The exact base commit/test overlay and stdout are retained. This is a focused
local race control: the ephemeral SDK executable and native stores were not
preserved, and no hermetic source inventory is claimed. It does not qualify
OS SIGKILL, concurrent-writer GC, lease/TTL or limit combinations, full matrices
or24h. The original manifest-loss/no-loss retirement controls also run through
the same helper; see the baseline log for their results.
