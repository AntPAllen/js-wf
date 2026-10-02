# Protobuf-to-JSON writer rollout reversal

Real R3 race fixture passes8.242s package/7.21s test. Two cases use production
lease acquire/release and SDK payload production before a higher-epoch JSON
writer resumes. An unfinished protobuf request executes the effect again
(two total executions), while a recorded protobuf completion replays without
another execution (one total). Both finish with four entries/one terminal,
unchanged retained prefix identities/payloads and passing all-peer audits.
The replacement's physical terminal bytes are JSON. CI requires the compiled
name of this test before execution. This is upgraded-reader writer reversal,
not downgrade compatibility with old JSON-only binaries or a full fault matrix.
