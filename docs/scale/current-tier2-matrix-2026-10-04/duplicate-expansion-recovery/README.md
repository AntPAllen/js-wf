# Verified recovery of duplicate Tier2 expansions

Only the positive raw expansions for seeds73–84,85–96,97–108 and109–120 were
removed after verifying pushed proof commit `feda512`, all committed parts and
concatenated digests, every canonical archive member, every ZIP member and every
expanded input hash. No visible process descriptor referenced those directories.
The exact executed script and per-directory report are retained here.

Recovered exclusive allocated file bytes: **2,464,092,160** (about2.29GiB).
Original ZIPs, canonical proof archives, actual model executables and pushed Git
proofs remain retained. Failed originals and qualification scope are unchanged.

To restore a raw expansion, create its `raw` directory under the recorded root
and extract the retained `raw.zip` there after checking its SHA256 against the
report and canonical member inventory. Alternatively recover the `artifact/`
members from the canonical proof into that `raw` directory, removing the
`artifact/` prefix. Verify every resulting input against the retained independent
review before replaying it. Do not use the recovery script as a restore command.
