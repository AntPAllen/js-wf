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

Hosted36975249620 at2b2130f is terminal/success. Explicit compiled-name checks
precede actual execution. Reverse test passes6.20s with effects2/request-only
and1/completed; forward native, CLI lifecycle, regeneration and persisted Python
exchange also pass. Original logs, terminal metadata and vectors/bindings are
retained under passed-ci/. Full rolling/chaos matrix scope remains unproven.
