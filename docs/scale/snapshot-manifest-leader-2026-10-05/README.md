# Snapshot reconstruction roots read from leader — 2026-10-05

## Change

Default journal snapshot manifest reads now obtain the latest value and exact
stream revision through administrative `STREAM.MSG.GET.KV_WF_STATE`, with an
explicit context. A stale missing or older manifest can refer to a prefix whose
live records have already been purged; snapshot reconstruction must start from
the leader's manifest. DEL/PURGE and recognized delete markers remain missing.
Malformed manifests still fail closed. The state metadata handle cache, manifest
CAS writes, snapshot hash/anchor validation, purge bounds and existing budgets
are retained.

Snapshot object reads now reuse the previously qualified absence-confirmation
helper. Its new cached-handle entry point avoids additional Object Store binding
RPCs on normal snapshot reads. Modern SDK chunk delivery remains unchanged, and
successful data still requires the recorded SHA256. Administrative confirmation
is issued only when Object Store metadata reports absence.

Other state/input/blob read paths are unchanged. The existing integrity terminal
leader reader is unchanged. No stream AllowDirect configuration is changed.
No full/final-source matrix, natural follower-lag reproduction or historical
server-side failure causality is claimed.

## Native results

The regression uses a real R3 file-backed cluster. It appends120 entries,
compacts leaving16 live entries, appends80 more and compacts again. Only the
weak client KV Get response is replaced with persistent absence or the older
manifest; the actual leader requests and payload reads remain native.

- Baseline `d66e351` plus the captured new test: FAIL10.58s. Absence produces
  `expected index 0, got 184`; the old manifest produces `expected index 104,
  got 184`. Both retain the original bounded2s reconstruction retry. Snapshot
  port object absence also fails. Genuine DEL/PURGE and malformed controls pass.
- Corrected normal: PASS7.18s. Both manifest controls reconstruct200 exact records,
  recover the exact committed manifest revision2 and make two administrative
  reads/zero direct state reads/zero injected weak gets per control. Snapshot
  object recovery, genuine DEL/PURGE, malformed manifest and blocked custom
  prefix leader deadline/cancel controls pass.
- Corrected race: PASS7.85s with the same controls.
- Existing result absence control PASS4.70s; purge/lease boundary PASS4.60s;
  concurrent compactors PASS4.33s; snapshot moving during scan PASS3.99s.
- Existing concurrent append-before-purge `append_drop` and `append_ack_lost`
  bodies pass3.96s/3.78s. The `clean` fixture fails3.26s at line149 while initially
  populating objects (`nats: no responders available for request`), before its
  body. The original group remains failed. Exact clean-body replay PASS3.23s;
  the earlier setup failure is retained, without a causal attribution.
- Journal and natsutil package controls pass4.011s/0.004s.

No unchanged Tier1 state-machine bodies were rerun for these default real
transport adapter changes. Domain/legacy-server compatibility for this new
manifest helper and full/final-source matrices remain open; only the custom
API-prefix route is exercised by this new manifest regression.

## Preserved evidence

`baseline/`, `corrected/` and `race/` contain split verified archives. Their
actual executed SDK identities/build info, selected source archive and matching
before/after hashes, logs, terminal verdicts and original native fixture files
are retained. The test file is identical across all three captured sources.
Normal and race selected snapshots are identical; current repository Go/module
bytes match their executed snapshot. Build stamps correctly record modified
source; no clean final-commit build is claimed. Selected sources exclude
`docs/scale`, and do not close over every external module/toolchain/environment
input. NATS2.15.0 is linked into the actual SDK native test process.

Primary native stores and the additional retained result-regression fixture
are hashed after shutdown without reopening them. Existing ordinary compaction
controls use temporary fixtures, which their cleanup removes, including the
failed setup. The additional existing-control process live identities were not
captured; their exact binary paths/logs and immutable primary binary are retained.

Concatenate parts in numeric order, verify part and aggregate hashes from
`archive-verification.json`, then extract. Every archive member and part has
been read back. Original roots remain under
`/tmp/js-wf-snapshot-leader-{baseline,corrected,race}-20261005`.
The opt-in test uses a fresh `WF_SNAPSHOT_LEADER_ROOT` parent and refuses an
existing named fixture directory. Producer/review scripts and executed review
are included. No live soak stores were reopened or restarted.
