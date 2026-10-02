# R5 per-request device-mapper delay fixture

The `block_delay` row uses the same private512MiB loop/ext4 node-four store
binding as `block_disk`. It adds an actual `dm-delay` table for100ms on reads,
writes and flushes for five seconds every30seconds. The table syntax follows
[the kernel dm-delay documentation](https://cdn.kernel.org/doc/html/latest/admin-guide/device-mapper/delay.html).
A dirty-file write/sync on that filesystem must take at least100ms while the
delay is active. The original linear table is then restored on the same backing
device. Every cut admits both R5 dispatch and journal leaders on that store and
waits for R5 readiness after restore. Mixed workloads, raw enabling-event p99,
histories, retained audits, immutable outcomes, physical queue drain and the2m
production file-store sync interval stay required.

Restoration uses an independent context on cancellation or setup failure. Table
switches resume even if suspend/reload outcomes are uncertain. The probe is
joined before its descriptor closes. The opt-in native capability test exercises
successful delay, cancellation with an active target, a subsequent usable write,
and removal of the private image/mount. Hosted CI must pass that capability test
before the mixed row. Unsupported kernels fail capability; they do not silently
substitute process delay or mapping suspension.

The independent row guard joins the writable Docker bind, both R5 leaders,
actual table, probe times and same-device restoration. Twelve delay-proof cases
reject short intervals, escaped syncs, wrong delays/targets/offsets, truncated
tables, changed backing devices and reversed boundaries. All35 Tier3 Python
tests and both controller timing tests pass. Both Go packages compile with their
native opt-in tests skipped; no native acceptance is claimed. This VM's kernel
6.12.93 exposes no delay target, and `sudo -n modprobe dm_delay` reports the module
missing. Native evidence must come from the hosted capability/smoke run.

This is one individual R5 row. It does not close combined I/O/process/reply cuts,
200 consecutive full-matrix seeds, power-loss durability or the24h full matrix.
The entire release scope remains required.

The [hosted capability plus35s smoke](https://github.com/AntPAllen/js-wf/actions/runs/36945872372)
was dispatched at source246ed70e0eedc76857163dac51d5249891b76484.
`smoke-launch.json` retains the original queued API response. The native capability
and smoke now pass; [complete verified evidence](hosted-smoke/README.md) is retained.
The current independent guards also pass. Sustained acceptance remains pending.
