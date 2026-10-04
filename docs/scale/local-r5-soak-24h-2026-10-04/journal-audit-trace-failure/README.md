# Instrumented actual 24-hour journal soak: failed attempt

Executed source: `18ddf0454de13a2d5df2c1a8b22efc5f765f2de0`.
The race-instrumented journal/seed1 test requested 24 hours and failed after
798.65 seconds at checkpoint batch100 / invocation cutoff2800. The user service
is terminal failed with MainPID0. This attempt qualifies neither a 24-hour row
nor the full matrix. No restart or gate relaxation follows this result.

## Independently verified evidence

- All 4,937 original archive members / 263,756,084 uncompressed bytes match the
  producer manifest. Complete archive: 69,995,410 bytes in three ordered parts.
- All 1,207 selected source files match the executed Git revision and unchanged
  pre/post captures. The actual race SDK is retained; its SHA and every build-info
  field, including the Go-version header, match.
- The failed checkpoint's three attempts took 8.019572, 20.003247 and 8.092592
  seconds. Overall elapsed time was38.097082 seconds. Each retained the original
  20-second deadline; the three-attempt limit stopped below the60-second cap.
- Attempt2 completed31,154 WF_JRN GetMsg calls with zero journal-read errors.
  It reported2,737 journals /29,188 entries /2,736 terminal records, then failed
  terminal-state validation with `context deadline exceeded`. This does not
  establish that terminal state was absent. WF_STATE Get had2,751 calls /15 errors.
- Attempts1 and3 failed journal reads (6 and96 read errors respectively).
  Zero reported journal counters in those attempts do not mean no data was read.
- Later batch104 cancellation follows the primary checkpoint failure.

This evidence identifies audit scan throughput as a concrete constraint under
these conditions. It does not confirm corruption, a NATS defect, the cause of
all read failures, or the exact cause of the earlier batch110 failure. The latest64
completed calls per attempt are bounded method traces, not a complete wire trace.
Original stores are retained but have not been independently reopened.

## Reproduce the review

`parts.json` records each part's size/hash and the concatenated archive hash.
Concatenate `originals.tar.gz.part-00`, `-01`, `-02` in that order, verify their
hashes and the whole archive hash, then extract into a fresh review directory.
The archive includes SDK, selected sources, raw reports/traces and original stores.
With a repository containing the executed revision and Go installed, run:

```bash
python3 review.py --root /path/to/extracted-originals --repo /path/to/js-wf --output /tmp/audit-trace-review.json
```

`review.py` verifies all original member hashes before evaluating local report
bytes against their archived hashes; it also verifies the captured sources
against Git and the actual SDK build info. `failure-review.json` is the accepted
review output. Adjacent trace/audit copies are conveniences; complete originals
are inside the split archive. The failed original producer directory remains at
`/tmp/js-wf-soak-journal-audit-trace-20261004`.
