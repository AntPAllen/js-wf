# Normal 100k streaming journal-fault capacity

Executed source: `878c34184c2b63ddf96f4b24141e42cb74bf8ef2`.
Actual normal SDK: `b1e06b09575199d8674383bc7d9b7c5b6299cb6ea468bcb3c22d7d65242476ab`.

The same file-backed three-node fixture contains 100,000 invocations, 1.2 million
journal records and 100,000 terminal values. Every publish acknowledgment is
checked. A complete combined streaming/watch-state baseline must match before
fault injection. The actual R3 memory consumer leader shuts down at callback 128
with 1,199,488 records pending. Recovery visits all 1.2 million records and returns
the exact complete report in 11.674917338 seconds under the unchanged 20-second
attempt deadline. Both temporary stream consumers clean up under named assertions.
The normal build and GOMAXPROCS=2/GOMEMLIMIT=2GiB are verified against the actual
live SDK process, now terminal.

All 2,894 selected input hashes, 64 local Git source files, runner, actual SDK,
all Go build-info fields and raw result rows independently verify. The complete
archive contains 3,328 members / 597,215,805 bytes; compressed size 110,317,753
bytes, split into five numbered parts. All original/member/part/concatenation
hashes read back. See manifest.json and independent-review.json. Concatenate
the numbered parts to recover proof.tar.gz, including the actual executable,
captured inputs, raw events and native stores.

This is native library shutdown, not OS SIGKILL. The state watch is not
interrupted. Stores are retained but not independently reopened. Race 100k,
legacy, five-container, full matrices and actual 24-hour qualification remain
separate requirements. Default readers and audit budgets are unchanged.
