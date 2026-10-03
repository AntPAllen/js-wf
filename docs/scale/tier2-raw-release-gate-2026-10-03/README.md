# Tier2 release gate requires uploaded raw execution events

`check-full-matrix.py` formerly returned
`clears_tier2_200_seed_gate=true` after validating job metadata and textual logs
for all 13 rows × 200 seeds, even when no raw artifacts were supplied.
The CLI made `--artifacts` optional, allowing that preflight result to be
mistaken for release qualification.

Metadata/log preflight now always leaves the release flag false. Raw artifact
qualification requires every named-test and package completion event, full
requested duration, and exact agreement with each log-derived row result.
Only successful raw verification may promote the 200-seed flag. The CLI rejects
200-seed requests without `--artifacts` before reading inputs. Smaller metadata
preflights remain available with an explicitly false release flag.

Artifact filenames are indexed in one traversal, preserving duplicate detection
while avoiding 2,600 complete directory scans for the full release matrix.

Eight tests pass in 2.325 s. They exercise all 2,600 synthetic raw event files,
missing/duplicate/incomplete/disagreeing artifacts, a failed final seed, unchanged
preflight after rejected promotion, and the CLI's mandatory-artifact boundary.
The retained old-source semantic control shows true → false for a 2,600-execution
metadata fixture with no raw artifacts. **These are synthetic verifier controls,
not actual workflow executions or evidence that the real 200-seed gate passed.**

All source/control/log archive members were reopened and SHA256 checked before
publication. Existing recorded-source campaign evidence is unchanged. The real
full matrix still requires terminal successful jobs and reviewed artifacts.
This gate verifies recorded runtime assertions; it does not independently reopen
physical broker stores or qualify the 24-hour soak.
