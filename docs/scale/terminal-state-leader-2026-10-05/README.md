# Terminal point-state leader reads

## Change and admission

Non-snapshot terminal checks use the documented administrative `GetLastMsg`
API for `KV_WF_STATE`, with no `DirectGet` option. They preserve API prefix/domain,
trace callbacks, original bounded audit retries, and deletion/expiry semantics.
Initialization is shared safely across concurrent journal checks.

Pinned nats.go1.54.0 modern KV.Get selects direct reads on AllowDirect streams.
The actual R3 fixture confirms AllowDirect=true. NATS documents that direct gets
may use followers without read-after-write coherence; administrative gets route
to the stream leader. See the [KV consistency documentation](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/key-value-store/README.md)
and [direct-get design](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md).

## Recorded results

| Campaign | Result |
| --- | --- |
| Old public checker, synthetic stale KV absence | FAIL4.70s: two wrapped reads; terminal state missing despite committed state |
| Corrected public checker, native file R3 | PASS4.38s: exact2 invocations/2 journals/8 entries/2 terminals, zero wrapped reads, two actual administrative requests and no state DIRECT.GET; real DEL/PURGE rejected |
| Modern native compaction/cohort/fresh-state controls | PASS39.11s |
| Modern native journal corruption controls | PASS9.93s |
| Legacy2.11.17 public compatibility | PASS42.79s; real three-process SHA dc3a94debfc18ee9762c783d941db36a8360955d9686c86bd660a9acc63701aa |
| Race native terminal regression | PASS5.31s, including concurrent shared-reader initialization; focused byte-fetch/initial-state/streaming-journal controls also pass |

Base Git source is713896287bf8621b4bae5a0e583b95668a3aca4d plus captured
correction/test overlays. Each corrected producer retains641 selected Go/module
inputs with matching before/after inventories, actual live SDK path/hash, full
build information, invocation/environment, original logs and stores. Modern R3
servers run inside the captured SDK executable (dependency v2.15.0). Race build
information explicitly records `-race=true`. This is a selected repository source
inventory, not an exhaustive compiler-input closure.

The baseline retains the failed binary/full build information/log/original stores,
640 source hashes observed after execution, and source bytes reconstructed from
the recorded Git base plus test overlay and verified against every hash. Its live
process capture was missed; independent source before/after matching is unavailable.
The baseline directory also retains the initially built corrected binary.

The corrected normal producer first failed before launching because its copied
binary lacked executable permission. This preparation error is retained separately;
permission was restored before the successful test campaign started.

## Proof and limits

`baseline/`, `corrected/`, and `race/` each contain split canonical archives and
`archive-verification.json`. Every archived member and published part is read back
and hash verified. Producers/source overlays are inside their archives. The
preservation script is included alongside this README.

The injected stale response is a client control; no actual follower lag or
historical partition fault is reproduced here. Historical failed campaigns retain
their verdicts and unconfirmed cause. Snapshot-watch mode is unchanged; its
consistency admission remains separate. This does not qualify arbitrary mirrored
KV layouts, runtime KV paths, R5/final-source matrices, 24h soak or million-timer
physical drain. The two existing live campaigns continue on their isolated sources;
these tests share VM resources with them.
