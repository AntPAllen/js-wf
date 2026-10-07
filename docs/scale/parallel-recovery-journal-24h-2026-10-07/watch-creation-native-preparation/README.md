# Real WatchAll creation-stall component

The opt-in `TestStateCreationNativeRecovery` uses three actual embedded stock NATS2.15.0 file-store peers and production nats.go1.54.0 `WatchAll`. An exact stream-scoped TCP barrier holds the first modern KV_WF_STATE consumer-create request before any bytes reach the broker, while metadata requests flow normally. The real two-second candidate creation watchdog must cancel that synchronous request, preserve the original20s deadline, join/close the first transport and obtain a fresh complete snapshot on the healthy connection.

The case requires1024 exact retained payloads, native deleted/purged tombstones and cohort exclusion, one creation error followed by one native initial-set barrier, exactly two attempts, zero remaining consumers and full untruncated TCP transcripts. A serial healthy snapshot control is checked under the same20s parent stage. The blocked creation must take2–3s; the parent budget includes retry, snapshot, payload/control verification and consumer cleanup. Population has its own30s setup budget; the original SDK profile is race/count1/2m/twoGoCPU/1GiB.

The producer captures a clean committed source, selected Git Go/module and helper inputs, external Go dependencies/toolchain, exact observed live SDK argv/birth/executable/environment, before/after hashes, final logs and every complete closed fixture/archive member. A persistent `state-watch-creation` CI row executes selector/seeded controls and the native case. Hosted acceptance remains separate.

Selector race controls pass count5 (1.483s), including fragmented PUB/HPUB, look-alike payload bytes, release/cancel/block/upstream EOF, malformed packets, wrong stream tokens and stream-name-prefix isolation. A normal integrity compilation with the native opt-in absent passes0.007s by skipping the native case; this is compilation only. Source-bound native execution and independent raw proof review are still pending.

Run from a clean checkout:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/run-state-creation-native.py --root /tmp/state-watch-creation-new
```

This is a controlled R3 request-cancellation component. It does not reproduce the original R5 server/watch stall, establish its server-side cause, validate the complete cutoff116480 cohort, or clear24h/default/fullmatrix requirements. The [failed original](../terminal-failure/) remains unchanged. Next is the complete copied cohort with fault interactions under its original20s budget.

The first actual native execution fails in bucket setup before reaching the fault; [failed original](../watch-creation-native-initial-setup-failure/). A fresh-source correction adds explicit metadata-leader readiness within the unchanged30s setup deadline. Native recovery acceptance remains pending.
