# Stopped full simulation campaigns — 2026-10-09

Neither campaign qualifies the complete suite. On inspection, no corresponding runner/test process remained and both original tool handles were missing. Original retained roots and isolated source checkouts remain unchanged.

## Full 146-family race: FAIL

Frozen `2b71f1d` finished with package FAIL after 6,930.377 seconds, inside its 180-minute suite budget. Of 146 seeded families, 144 completed all 1,000 bodies. The combined Signal runtime failed replay at seed 645 (644 complete bodies); the original Signal runtime failed generation at seed 971 (970 complete bodies). Both diagnostics say `worker failed to suspend: context deadline exceeded`. No test-alarm panic or DATA RACE report appears in the output.

This source still has the ten-second per-schedule wall-clock watchdog. Later `55eeb77` increases that watchdog to one minute; this older failure does not qualify that change or prove absence of another defect. All virtual transport and domain recovery deadlines remain unchanged.

The shared `failure-trace.json` was overwritten by the later failure; only seed 971 survives. This lost diagnostic is explicit. Signal runtime failure capture now uses a unique filename containing the test and seed, including distinct files for repeated failures of the same seed. It leaves the requested filename prefix untouched, and reports the actual path for `FAULT_TRACE` replay. Other workloads retain their existing capture behavior.

## Full 148-family normal: interrupted

Frozen `55eeb77` has 39 completed seeded families, followed by entry into `TestSeededGraphSignalRuntimeCombinedReplay`, with no package verdict, coverage summary or time result. No live process remains. Interruption cause is unconfirmed. This is neither PASS nor a model/test FAIL.

## Evidence review limits

`executed-review.py` verifies selected files against their exact Git commits and the separately retained source checkouts, checks retained binaries and event hashes, and copies available metadata/events/trace. Both supervisor `source-after.json` records are missing. The review records a fresh observation on October 9 rather than manufacturing a historical source-after record. It does not infer complete qualification from partial seed counts or the absence of race reports.

The full current normal/race and extended suites, remaining canonical runtime/deployment migrations, native matrices, actual 24h soak, million physical drain and adoption/release requirements remain open.
