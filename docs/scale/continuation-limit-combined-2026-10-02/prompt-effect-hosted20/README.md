# Prompt forbidden-effect detector: hosted budget 20 acceptance

[Run 37045824031](https://github.com/AntPAllen/js-wf/actions/runs/37045824031)
passes at exact `2a301654e7a57c1f66dacdf14b2f36d3bcb10ef6`.
Independent inspection verifies source hashes against Git, the exact three
compiled worker guard substitutions, actual race parent/package verdicts and
all three positive cuts. Their recovery times are 17.193, 13.039 and 12.978
seconds with the unchanged production 12-second lease TTL and strict 30-second
gate. Each cut has an actual worker SIGKILL and graceful shutdown/restart of all
three NATS servers with retained stores.

Raw journals preserve each cut prefix and end at exactly 20 entries with the
limit failure. Checkpoint index 13, SDK offset 12, two continuation stages,
15 played/recorded steps, raw integrity audits and zero positive/offline effects
verify. Replacement epochs increase where a successor append is needed.

The compiled suffix-budget mutation fails on its first actual forbidden effect,
with exactly one synced effect and 20 entries ending at the forbidden request.
No compile failure or timeout substitutes for the expected detection.
All 56 originals are losslessly archived; every member's SHA256 was checked on
readback before atomic publication. See `manifest.json` and
`independent-review.json`. Originals remain in
`/tmp/js-wf-continuation-prompt-hosted20-37045824031`.

This accepts the corrected small-budget contract. Production-cap run 37045827952
is a separate active qualification. Neither this contract nor its negative
fixture closes deterministic combined-cap modeling, full matrix or 24-hour soak.
