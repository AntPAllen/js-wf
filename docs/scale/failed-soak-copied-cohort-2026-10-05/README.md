# Complete audit of disposable checkpoint1040 store copies

Executed source: `e8bce877d9408d6b2743c4c0b62c1f96374d4ecc`.

One complete streaming journal/state audit through invocation sequence29120
passed in **5.184059489 s**, under the original20s attempt budget. Native test
PASS17.27s includes cluster startup/cleanup. Report: **29120 invocations,
29120 journals,321151 entries,29120 terminals**. The checker freshly reads all
retained journal records and filters the captured cohort; delivery trace covers
321746 records/48299508 bytes without Next errors, alongside29120 invocation
records. This is the complete original cohort, not a small synthetic substitute.

The matrix client policy is used. An initial KeyValue lookup timed out at2s;
the existing retry succeeded. WatchAll creation completed in10.027ms, WatchStop
cleanup in2.900ms. The tracer preserves native update channels and does not
consume updates. Full checker success requires the initial-set barrier; cleanup
success alone does not certify it. No contexts/retries/budgets were extended.

Independent review verifies actual SDK executable/clean Git build, all five
actual Docker process executables/build fields and copied-store mounts,
659 selected Git source inputs before/after, and3706 original store files
against the complete original archive. Original bytes remain unchanged; only
disposable copies were opened. All4197 archive members and nine parts were read
back and verified. Archive SHA256:
`3ead8a31f083b468ce1cf4c19d3ec89c96579639da978a8503456af5024799db`.
Producer/reviewer/preserver, SDK/server executable bytes, source, native logs and
copied stores are retained. This native diagnostic shared the VM with the live
million-timer candidate; no pressure or speedup cause is inferred.

## What this establishes

The retained failed-soak cohort can pass its full invariants and complete audit
within20s without concurrent runtime writers or injected faults. This does not
reproduce or fix the original faulting soak, confirm its cause, qualify full
matrices, or establish an actual24h pass. The original failed parent staysfailed.

## Remaining budget in the original failed audit

The separately copied original trace/checkpoint files match their previously
preserved archive hashes. Attempts2/3 had only **1.861s /0.863s** remaining after
journal consumer cleanup. Source places the initial state watch next, so its
available outer budget was bounded by those amounts. Original trace did not
observe WatchAll, its initial barrier or cleanup; this is an inferred bound on
the following phase, not proof of a blocked watch or its cause. The new tracer
records creation/cleanup. Original20s/60s/threeattempt and internal2s limits stay.
See `historical-remaining-budget.json` for timestamps and input hashes.
