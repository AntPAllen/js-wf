# Streaming audit API adoption

The four full/cohort streaming audit APIs now select the byte-bounded reader:
4096 delivered-record windows, documented SDK8MiB client buffer, source-matched
cursor replication and the unchanged frozen-bounds/checker/cleanup/two-resume
algorithm. Point reads and non-streaming bulk APIs retain their previous readers.
The existing integration streaming-state audit mode therefore selects this
reader without a new competing mode or changed deadline.

Adoption follows full R5 population/timing and explicit/short-success interruption
acceptance, refill-sized payload/hole controls and the separate native actual
leader-loss/cancellation/corruption campaign. SDK pointer-channel allocation,
single oversized-message/fallback limitations and total checker heap remain as
documented in the parent proof. No limit is claimed on all heap allocations.

## Public entry points verified after selection changed

The common native comparison now invokes all four actual public APIs, with the
point reader as report/error oracle. NATS2.15.0/R3 compaction/cutoff/fresh corruption
passes36.32s; protocol corruption/duplicate invocation passes8.22s. NATS2.11.17/R3
legacy compatibility passes57.28s, including compacted prefix, later malformed
journal exclusion, changed/missing terminal state, tombstone, corrupt/restored
snapshot and orphan journal. Full and captured-cohort public state APIs are also
called explicitly by the legacy fixture.

All three legacy server processes verify version2.11.17 and actual `/proc/PID/exe`
SHA256 `dc3a94debfc18ee9762c783d941db36a8360955d9686c86bd660a9acc63701aa`.
The copied legacy binary and build information are retained. Its complete build
source is not recaptured here. The current native servers share the captured SDK
test executable. Native source before/after639 selected Go/module inputs match;
actual live SDK hash/build information/source bytes/producer/logs/original stores
are archived. This is not an exhaustive compiler-input capture. Original stores
are retained, not independently reopened.

Focused byte/scan/streaming race controls pass3.411s; the race executable is not
separately captured and the full native campaign ran normally. Archive member
and part readbacks verify the published bytes.

The ongoing native-million diagnostic shares this VM. Final-source full matrices,
the failed historical campaigns, million physical-drain acceptance and actual24h
soak remain open. Next qualification uses this materially changed audit transport;
earlier source passes are not retroactively promoted.
