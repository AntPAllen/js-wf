# Eleven completed rows: partial current Tier2 qualification

Run 37050644803 at `3f483bd61fba121a38c5b42e3aa84d07265ecab0` remains active.
The eleven downloaded row artifacts pass the campaign revision's row checks:
exact test and package completion, at least ten minutes, retained-audit counts,
workload cells, aggregate/cell terminal latency and enabling-event progress.
Blockdisk and upgrade remain active and are absent from this partial archive.
No whole-matrix or 200-seed gate is cleared.

The eleven rows contain 29,232 invocations and 291 faults. Worst aggregate
terminal p99 is 15.010394 s; worst cell terminal p99 is 18.069686 s; worst
enabling-event progress p99 is 12.938768 s. These are recorded runtime audit
verdicts, not a new independent scan of physical retained stores.

The whole-matrix checker now accepts `--artifacts ROOT`, requiring exactly one
raw Go JSON file per expected row/seed, the named full-duration test and package
pass, no failure events, and a row verdict matching the Actions job log. Whole
acceptance still requires all thirteen terminal jobs and checkout revisions
through its existing metadata/log checks. The raw check alone accepts no
campaign. New tests reject missing/duplicate files, missing package completion,
and raw/log disagreement; all eighteen matrix tests pass.

For this partial review, the new raw check matches all eleven verdicts parsed
by the unchanged campaign-revision checker. All 578 campaign Go/module/workflow
files match the local source bytes. Hosted checkout attribution awaits complete
job logs; no uploaded source manifest is claimed. The saved metadata records
the live campaign, so the whole-campaign guard must reject it at this point.

All 745 files (original downloaded artifacts, review inputs, exact checker
copies and source inventory) are archived losslessly with SHA256 member readback
before atomic publication. Only regenerable Python bytecode caches are excluded.
The original local root remains available in `manifest.json` for qualification
of the final campaign when its remaining rows finish.
