# Actual24h journal row fails retained audit at batch110

Executed `20babb566534e3ea7aa06b5526a748c9d65c6ed5`, journal/seed1/race, requested24h.
The named test fails after950.33s. Batch100 eventually audits2800
invocations successfully; batch110/cutoff3080 exhausts all three20s attempts
within the unchanged60s total. Reports show3080 invocation records and zero
reported journals/entries/terminal. That report does not isolate the failed RPC
or establish that no journal data was read. Later fanout cancellation is secondary.
No audit budget change, automatic restart or confirmed NATS/runtime cause.

User service `js-wf-soak-journal-24h-20261004.service` is terminal failed, MainPID0,
and stopped-run archival has completed. Actual failed root remains at
`/tmp/js-wf-soak-journal-24h-20261004`. All1203 pre/post selected
producer sources match exact Git, actual race SDK SHA/build info verify,
and all4960 original member hashes /276,476,487 bytes
verify. Canonical originals72,084,648 compressed bytes /3 parts;
part and concatenated readbacks verify. Actual SDK/source/raw/physical stores
are retained; stores have not been independently reopened. Reassemble in part
order and verify canonical/member hashes before extraction into a fresh root.

This is failure preservation, not a24h row or full-matrix qualification. The old
launch snapshots remain historical; no process is claimed live from them.
Next work is to isolate which retained-scan or snapshot operation consumed the
budget, then fix and verify the cause before another24h attempt.
