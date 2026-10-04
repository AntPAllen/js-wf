# Actual 24-hour batched journal attempt: terminal failed

Exact `b287e983872e310d92bfb9dab263f0a01875a43b`, race/journal/seed1, explicit batched checkpoint/final
audits with trace, requested 24h. Named test fails after 3223.15s
at batch400 / cutoff11200. Unit terminal failed, supervisor/SDK stopped; original
five containers removed by fixture cleanup. No automatic rerun.

All three attempts reach their original deadlines: [20.00245, 20.002518, 19.997885]s;
total 60.003632s under the original three-attempt20s / total60s
cap (milliseconds of scheduling/trace completion overhead). Batch390 successfully
checks10920 invocations /120353 entries /10920 terminals.

Attempts1/3 complete242/244 WF_JRN fetches with zero errors; cumulative Fetch
elapsed14.661/12.685s includes caller processing, not pure RPC time. Validation
reaches6128/10114 journals before deadlines; third makes10128 terminal KV reads,
15 deadline errors. Its terminal-state-missing text carries context deadline
exceeded; it does not establish missing state or corruption. Attempt2 reaches
239 fetches with one error, then two leader fallback reads both time out. All
invocation scans complete22 fetches without errors. Concurrent method durations
may overlap; they must not be summed as wall time. No NATS-cause claim.

Independent review verifies all 5271 original members /
475521380 bytes, source-before/after and all1222 selected Git
inputs, actual race SDK SHA/build information including Go version header.
Complete archive: 99209919 compressed bytes in four SHA-verified
parts, digest `8c84519f9539835b19ba041afb1a4f2bb6b933a7efb8847d885681129dcd7f61`. Concatenate numbered parts, verify the
archive digest, then extract into a fresh directory. Original sources, SDK,
traces, events and broker stores remain retained; stores were not reopened.
`review.py` binds every analyzed byte to the original archive manifest.

The 24-hour row, full matrix and original million-timer gate remain open.
Next work compares bounded4096-message delivery windows with previous512 on
identical retained stores and the same full invariant body. Audit budgets,
cohort coverage, corruption checks and fault cadence stay unchanged.
