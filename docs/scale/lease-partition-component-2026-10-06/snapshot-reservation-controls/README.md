# Deterministic late I/O permit controls

At `cb48979`, six actual source-bound race binaries execute the original snapshot-abort test/two subcases with count1/3m/2CPU/2GiB. All original assertions, sleeps and deadlines remain. A single borrowed semaphore permit is returned after an actual waiter blocks; a five-second diagnostic deadline prevents a missing waiter from hanging the control.

| Fixture | Stock upstream | Contiguous refinement |
| --- | --- | --- |
| Original available-only drain + controlled late return | Expected native failure in both subcases | Expected native failure in both subcases |
| Reserve every permit + identical late return | Both subcases pass | Both subcases pass |
| Reserve every permit, no injection | Both subcases pass | Both subcases pass |

Original negative controls hold4095/4096 permits, observe the real semaphore waiter, then return the borrowed permit and fail the exact writer-returned-before-refill assertion with nil. Positive injected controls reserve4096/4096 and preserve both errSnapAborted/orphan-removal and adopted-snapshot assertions. No variant reports a race or skip. This demonstrates a reachable test setup failure without the catch-up refinement. The exact in-flight holder in the historical full170 failure was not captured; that native verdict remains failed.

Independent review binds 2025 Git/current/retained/before/after inputs, 2666 compiled dependencies, 598 unchanged original module files, consumer mod/sums, exact reversible test changes, actual SDK argv/birth/bytes/environment and every complete archive member. Full archive: 224,158,847 bytes/5,356 members, SHA256 `8e3930542b482b5458c538c84507e9cbf872d50d0dd7a460d1d2a05fc0172f8a`. The fixture source is fresh; no original retained broker is opened.

The production guard and default official v2.15.0 dependency are unchanged. Composing the qualified peer locking and semaphore reservation must be qualified before another complete derived170 comparison. No original-source full-suite, workflow matrix, Tier1 or actual24h gate is promoted.
