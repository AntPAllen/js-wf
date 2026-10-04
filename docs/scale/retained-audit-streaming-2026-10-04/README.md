# Streaming retained journal audits: native equivalence and100k comparison

Actual source `1d43a2b2cdf2fe82e3d1fcf5bbabf0c42ad38986` adds an opt-in
accumulator that decodes every eligible retained record but keeps protocol
state instead of decoded prefixes. Monotonic epochs require only the current
owner; terminal records release request/completion payloads. Memory scales
with invocations/current payloads, not complete prefixes. No inter-audit cache.
Stored semantic errors reduce in original sorted subject order after raw
decoding. Compacted subjects use original reconstruction/slice checker.
Original checkJournalRecords and CheckSnapshot bytes match283ba32; defaults
and the production harness are not switched.

Race unit differential controls cover1000 seeded valid histories, every prefix,
terminal errors/mismatch, twelve mutation classes, arbitrary mixed histories,
continuations and blank-owner/epoch transitions. A200001-entry prefix with100000
epochs adds no per-entry/per-epoch allocations. This is checker evidence, not a
new transport-simulation or runtime full-count release qualification.

## Native race controls

[Complete proof](native-controls/) verifies three top-level tests and five
corruption subtests. Point/batched/state/streaming modes give exact reports and
errors for compaction/cohort/fresh corruption, I1/I2/I3/orphan and terminal
replacement/Delete/Purge/recreation. All2893 captured inputs /63 Git files,
actual runner/SDK/all build-info fields and3981 original members verify.
Live/proc identity was not captured. SDK SHA256
`66bcdd32d496803b831c792020af5c5a4560c1c71cecb62ca6383976cff55654`.
Complete proof31842651 bytes /two parts /87949471 original bytes; SHA256
`0b0d0c6484d610f77d81f8d90defd747c80f2b10545d8f534bbba05b19984e42`.

## Same-store normal100k comparison

[Complete proof](native-100k/) runs four complete100000-invocation /
1200000-entry /100000-terminal audits on the same fresh stores,2GiB/GOMAXPROCS2,
under original20s limits. All four exact reports match:

| Reader | Time | Allocated bytes | GC cycles |
| --- | ---: | ---: | ---: |
| Baseline | 17.401056545 s | 4476760112 | 10 |
| Streaming, point KV | 17.299744988 s | 4264822056 | 17 |
| Streaming, fresh KV snapshot | 11.809901342 s | 3253372288 | 13 |
| Baseline recheck | 18.983371485 s | 4545273816 | 15 |

Streaming alone is close to baseline; combined mode is faster in this fixture,
about32% below the first baseline. This is one bracketed comparison, not a timing
distribution/general throughput guarantee. Allocation counters include client
and all three embedded servers, not checker-only allocations or peak RSS.
GC counts are not necessarily lower. No NATS-cause inference.

All2893 selected inputs /63 Git files, actual SDK/live/proc environment and
all build-info fields verify. SDK SHA256
`9f86c17ebf8862fa2c1a60ecf05544a2289f6f482dcc280909fb44014edeacb3`.
3325 complete original members /597135379 bytes; compressed110287055 bytes /
five parts; SHA256
`de66443a50b6e2a725bb6321d791245bf5a7e399a9ef17d93cbf9898700f57f9`.

Both proofs retain SDK/stores/source copies/events/exact producer and reviewers.
Every original/member and part/concatenation hash verifies by readback.
Concatenate ordered parts, verify manifest, extract into a fresh directory.
Stores were not independently reopened. Input inventory excludes exhaustive
assembly/embed/generated/hermetic proof. Candidate full-audit leader-loss,
state-watch fault recovery, legacy/five-container adoption and full matrices /
24h qualification remain open. Production/default audit mode and20s/60s limits
stay unchanged; no soak is restarted. Next qualify actual full-cohort recovery
under faults before exposing the combined mode in the production harness.


## Native journal-fault recovery

[Small race controls](journal-faults/) pass both state modes through actual
pending R3 consumer leader library shutdown and cancellation. Exact500/2000/500
reports recover in0.557s/0.445s; cancellation stops at128.

[Large race control](large-journal-fault-race/) passes12000/144000/12000 full
baseline and recovery after actual leader shutdown with143488 pending, in10.745s.
Both controls require temporary consumer cleanup and retain complete verified
originals. Larger live-process identity captured; small controls omit it.
State-watch interruption, OS SIGKILL,100k fault capacity, legacy/five-container
and complete release qualification remain open.


[Normal 100k fault control](large-journal-fault-100k/) extends the same test to
100000 invocations /1.2M journal entries /100000 terminals. Actual R3 consumer
leader shutdown leaves1199488 pending; full report recovery takes11.675s under
original20s. Complete baseline/cleanup/source/executable/live environment and
originals verify. This closes this normal native journal-fault capacity check;
state-watch interruption, OS SIGKILL, race100k, legacy/five-container and full
release qualification remain open.


## Native state-watch interruption

[Full race controls](state-watch-faults/) pass real KV WatchAll consumer-leader
loss and cancellation at delivery128 inside12000/144000/12000 audits. The
SDK-created watch consumer is single-replica;11125 pending at shutdown. Complete
recovery takes12.807s; cancellation returns invocation-only count at128 in11.394s.
No synthetic values or completion marker; all source/live SDK/build-info/original
proofs verify. Legacy/five-container/OS SIGKILL/full matrices/actual24h remain open.
