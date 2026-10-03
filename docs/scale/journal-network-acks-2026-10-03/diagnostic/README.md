# Real journal TCP acknowledgment recovery fixture

The new opt-in `TestThousandJournalNetworkLostAckRecoveries` applies actual
TCP faults to all attempts: alternating committed publications with held/dropped
responses and connections cut immediately before publication. NATS produces
the errors; the wrapper never synthesizes an acknowledgment or failure.
The fixture kills the confirmed journal leader halfway through, restarts it
on its original store, then compares every raw outcome on all three peers.

The corrected pre-commit two-case diagnostic passes. Independent raw review
checks the full TCP transcript against forwarded counters, real publication
payload/CAS bytes, retained generations/sequences, absent/committed retries,
leader movement and all cross-peer receipts. Seven mutations of those actual
inputs are rejected: inflated count, escaped response, buffer overflow, missing
peer receipt, changed original sequence, truncated traffic and failed execution.
This does not clear the 1,000-case requirement. No executable/store retention
or complete pre-build source inventory is claimed for this diagnostic.

The initial diagnostic intercepted only `PublishMsg`; production uses `Publish`.
It was correctly rejected before any fault, and its original failure/transcript
is preserved. The wrapper now intercepts both paths. Complete default-size
execution remains required. Run with `WF_JOURNAL_NETWORK_ACK=1` and set
`WF_JOURNAL_NETWORK_ACK_REPORT` to a fresh artifact directory. An explicit
smaller even count is diagnostic only.

All 24 archived members were reopened and SHA-verified before atomic rename;
actual fixture/reviewer bytes and original failure are retained with the raw
diagnostic. Physical broker stores were automatically removed by the fixture.
