# Normal-build 100,000-invocation audit: original deadline failure

Actual retained SDK at `6a38a7c41d040cbf6d259f98dcd0a7fc4a9704b7` runs
TestBatchedInvariantAuditNativePhaseProfile with explicit100000 population,
GOMAXPROCS=2 and GOMEMLIMIT=512MiB, without race instrumentation. Fresh real
three-node file-backed embedded servers use stream-matched audit consumers and
512-message batches. All1.3M publication acknowledgments and terminal puts
complete before profiling starts; original fixture/whole-test lasts137.66s.

The unchanged20-second audit fails at20.092516379s. Both complete stream scans
visit their exact expected counts:

| Stream | Records | Payload bytes | Scan wall time | Visitor wall time |
| --- | ---: | ---: | ---: | ---: |
| WF_INV | 100000 | 500000 | 1.923800261 s | 0.073243178 s |
| WF_JRN | 1200000 | 84700000 | 17.208463306 s | 2.520930176 s |

Partial report is100000 invocations, zero validated journals/entries/terminals.
This means validation did not complete, not that journals or terminal state
were absent. Scan timing includes visitor decoding/grouping; CPU profile
includes all three embedded servers and client in one memory-limited process.
GC scanning/assistance occupies substantial CPU samples. This identifies
memory/retained-record cost as a next hypothesis; it does not establish the
five-container client cost or a NATS defect. Normal build alone does not solve
large-cohort capacity. No budget or production config is changed.

Independent review verifies all2889 selected inputs/59 Git files, runner,
pre/post hashes, SDK digest/all build-info fields, captured live/proc SDK,
exact named-test failure, record counts/bytes and readable CPU profile.
SDK SHA256: `84564d8c5f8746174e4b96fb0f5e647974ccb4e3763d554511d0b96289325dbb`.
All3326 original archive members /594575584 bytes, originals after archiving,
all five parts and concatenation hash verify. Complete proof is110062697 bytes,
SHA256 `22a0d1c7a8acdb8fce87177fabd1d2ccb4010637dfa6eedbfc252c28cc446e28`.
Retained stores were not independently reopened. Selected-source inventory is
not exhaustive assembly/embed/generated/hermetic proof. SDK is terminal; no
soak restarted or release requirement qualified.

Next compare a measured memory budget on the same100k logical fixture under
unchanged20s, retaining original512MiB failure. This is test-only memory
sensitivity measurement, not permission to substitute an easier release
profile. Continue toward bounded-memory complete audits, captured source
matrix qualification and actual24h gates.
