# Full streaming audits under native KV watch faults

Executed source: `722305aeaebfd6ae1b1363ebef77efebaec2f80b`.
Actual race SDK: `40b1079460e1cc3f813cdab81bff256d4beacd10c94ff0dc45733cbbb0c36c42`.

Both fresh three-node file-backed fixtures contain 12,000 invocations, 144,000
acknowledged journal records and 12,000 terminal values. Each requires a complete
combined streaming/watch-state baseline first. A relay forwards real WatchAll
entries to the checker and supplies no synthetic values or completion marker.

At delivery 128, the recovery case identifies the single actual watch consumer
and shuts down its real native server leader with 11,125 records pending. This
SDK-created ordered watch consumer uses one replica. The full exact report
recovers in 12.80705601 seconds under the original 20-second attempt deadline.
23,019 watch deliveries were observed in total; this does not claim a single
uninterrupted watch. Cancellation at the same boundary stops at exactly 128
observed deliveries and returns context.Canceled with only the invocation count,
in 11.393537711 seconds. Relay Stop releases the native buffered callback.

All 2,895 selected captured inputs, 65 Git-local files, runner, actual binary,
live environment and every Go build-info field independently verify. The
complete archive contains 3,639 members / 213,645,530 original bytes; compressed
size 52,273,791 bytes across two numbered parts. Every original/member/part and
concatenated digest reads back. See independent-review.json and manifest.json.
Concatenate numbered parts to recover proof.tar.gz, including the actual SDK,
inputs, raw events and native stores. The actual SDK process is terminal.

Library shutdown is distinct from OS SIGKILL. Stores were retained, not reopened.
Legacy, five-container/full-matrix, 100k state-watch faults and actual 24-hour
qualification remain open. Default audit readers and 20s/60s limits are unchanged.
