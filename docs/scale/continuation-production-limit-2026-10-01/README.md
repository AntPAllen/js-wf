# Production 100,000-entry continuation boundary — October 1, 2026

The existing real R3 continuation limit fixture is shared between its per-push
16-entry budget and a new opt-in actual production cap. The large case never
assigns maxEntries: both workers must report the unchanged journal.MaxEntries
100,000 default. Production runtime source is unchanged from 7e414b2.

The initial and middle stages each run 24,996 additional SetState steps against
one padding key. Both checkpoint and archive publication use real replicated
JetStream and Object Store. The first worker reaches finish_v1 and suspends on
a signal with exactly 99,997 reconstructed logical records. The verified finish
anchor is index 99,993 and its absolute SDK offset is 99,992. Prefix polling waits
for stage entry before reconstructing history, avoiding repeated full-prefix
reads while filling the cap. Progress logging can repeat when a handler replays.

After the first worker stops, a replacement pinned to another peer denies
archive reads. A gate signal consumes index 99,997 and completes its SDK wait at
99,998. The next effect request cannot reserve completion plus terminal slots:
Failed occupies the final legal index 99,999, retains LimitRequest and never
executes the effect. Both prefix stage counts stay unchanged after replacement.
Full logical reconstruction contains exactly 100,000 entries. Terminal state
matches the journal byte-for-byte, every peer returns ErrTooLong and raw-state
integrity finds one invocation and one terminal.

Offline audit uses the existing CLI limit protocol: explicitly substitute the
retained rejected request for Failed, then replay both named continuations and
all 99,995 SDK entries. It must stop at ErrReplayPendingStep with no effect.
A changed rejected declaration must fail nondeterministically. This does not
claim the SDK replay API automatically interprets Failed.LimitRequest.

## Validation

- `production.log`: full real 100,000-entry case passed in 67.861 seconds, one
  initial/two middle handler entries before capture, zero prefix entries after
  replacement, zero archive reads, one frame read and zero rejected effects.
- `small-race.log`: shared 16-entry fixture passed under race in 18.045 seconds.
- `suffix-budget-mutation.log`: a compiled overlay counts live suffix length
  instead of absolute indices; the shared small fixture incorrectly completes
  and fails its required ErrTooLong assertion in 16.348 seconds. This is a
  behavioral negative control on the small fixture, not a 100,000-entry mutant.
- `vet.log`: worker vet exited zero without diagnostics.
- `suffix-budget.patch` and `SOURCE_SHA256SUMS`: control and tested sources.

Run the large case with:

```
WF_CONTINUATION_BOUNDARY=1 go test ./worker -run '^TestContinuationProductionJournalLimitBoundary$' -count=1 -timeout=25m -v
```

The manual journal-boundary-100000 workflow now has independent raw-journal and
continuation jobs. Hosted validation is launched after this commit. The large
case is intentionally opt-in; routine CI retains the small race fixture.

This closes the actual production continuation cap/reserved-effect slice.
Seeded integrated limits, limit-adjacent crash/unknown-write cuts, other remaining
continuation acceptance gates and final-source full matrix/24-hour soak remain
open. No production limit or runtime behavior was changed. Existing million
timers and full Tier 1 runs remain running without restart.
