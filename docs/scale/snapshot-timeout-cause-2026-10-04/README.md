# Preserve a snapshot lookup timeout's cause

[CI run 37168567962](https://github.com/AntPAllen/js-wf/actions/runs/37168567962)
at exact `32651f0`, job **111336618321**, fails the combined continuation/promise
`after_manifest` cut after restart: `journal gap or corrupt entry: snapshot
object "snapshot-4220b09ac77e6215-10-5e4cf0d409ea1e35": <nil>`. That cut fails
at 14.33 s; the whole eight-cut test fails at 135.37 s while seven other cuts
pass. Its original artifact **11290178716** contains only a 5,873-byte console
log archive. No actual executable or physical stores were uploaded or preserved;
this evidence cannot establish missing/corrupt bytes or an underlying server cause.

An actual compiled Go overlay against source-identical original reader code
reproduces the exact nil-cause diagnostic in **2.007 s**: the first object read
uses the entire two-second retry context and returns its deadline error while
the outer context remains live. The error path discards that error when no
previous failure exists. This demonstrates a reader diagnostic defect, not a
reproduction or explanation of the original server-side timeout conditions.

The reader now retains that first error when the retry context expires. A
previous missing-object or digest observation remains intact. Data verification,
two-second visibility bound, parent cancellation and ErrGap/typed object-gap
classification are unchanged. This is an error-cause fix and does not turn the
failed combined test into a pass or authorize a deadline relaxation.

Two real deadline controls pass normally in **4.009 s**; the complete journal
race suite passes in **5.031 s**. Restoring exact original production source via
compiled Go overlay fails the first-lookup control in **4.007 s**, while the
prior-missing-object control passes. Five inputs match before race/control and
afterward; focused normal execution preceded that ledger. Test executables are
not retained. Original failure metadata/log/upload, sources, diagnostic overlay,
controls and hashes are in `proof.tar.gz`; every member was read back and verified.

The production source fingerprint changes. The previously accepted `9ecc37c`
121-workload normal100k/race1k remain historical qualifications; new full gates
are required at the updated source. Full Tier2/Tier3 matrices, original million
timer drain and the actual 24-hour soak remain open. Other live campaign jobs
are kept for their exact executed-source evidence and are not silently promoted.
