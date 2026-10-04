# Fresh KV watch audit candidate: native equivalence and100k comparison

The opt-in reader obtains a new initial latest-value WatchAll snapshot every
call, using the existing bounded read/retry helper. It requires the documented
nil completion marker and stops/drains the watcher. Closed partial delivery,
cancellation, malformed identity/revision/operation and out-of-order revisions
fail closed. Delete/Purge matches Get's missing-key behavior. Captured cohort
keys exclude later invocations; no values/results survive between audits.
Original checkJournalRecords and modeled CheckSnapshot bytes are unchanged.
Existing default point/batched readers and the production harness stay.

## Native race controls at9b2a182

[Complete control proof](native-controls/) verifies three top-level tests and
five corruption subtests. Point, batched and snapshot readers give identical
reports and exact errors for compaction, cohort exclusion, fresh corruption,
I1/I2/I3/orphan cases, terminal replacement/Delete/Purge/recreation and full
retained state. All2891 captured inputs/61 Git-local files, retained runner,
actual SDK/all build-info fields and3979 complete original members verify.
Snapshot checker bytes match283ba32. Live/proc identity was not captured for
this run. SDK SHA256:
`40899ec1f351dfb865f1172ca3981f463b166c9cad13cbfdf30d18aed7cf99de`.
Complete proof31870908 bytes /two parts /88578713 original bytes; SHA256
`b4eb5abb1e8d64e5d8d2e80a43c555a5fa32076dc7bf9b572fb9d02e61db4f7f`.

## Same-store normal100k comparison at9b2a182

[Complete comparison proof](native-100k/) uses the same1200000-entry /
100000-terminal stores with2GiB/GOMAXPROCS2 and original20s attempts:

| Reader | Full audit |
| --- | ---: |
| Point-state reads, before | 15.043925816 s |
| Fresh state snapshot | 14.468247646 s |
| Point-state reads, after | 19.93021917 s |

All three exact100000-invocation/journal/terminal and1200000-entry reports
match. Before/after variation prevents a reliable speedup claim. Normal SDK
`9de02c50db5b923e1967f53ba3bd28dc557f9e80031f7e614827ed31d6de53ad`,
live/proc SDK identity and every build-info field verify. All2891 captured
inputs/61 local Git files and3323 complete originals /594608683 bytes verify.
Compressed109965360 bytes /five parts; SHA256
`454343be321f38b03c817e7921de198cdfa7fa45f570a61f978c03cd70bd46d1`.

Both proofs include SDK/stores/source copies/events/producer and reviewers.
Every original/member and part/concatenation hash verifies by readback.
Concatenate ordered parts, verify manifest, extract into a fresh directory.
Stores were not independently reopened. Selected inputs exclude exhaustive
assembly/embed/generated/hermetic proof. No candidate leader-loss/recovery,
legacy-server capacity, five-container adoption, reliable performance gain,
full-matrix/race/24h release qualification follows. Original budgets remain.
