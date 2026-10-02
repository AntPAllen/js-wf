# Mixed production CAS mutation

The shared fixture admits four held shorts, three suspended timer parents, two
suspended signal parents and a suspended six-child/twelve-grandchild fan-out.
It SIGKILLs the observed journal leader and verifies the exit signal. Two production
journal stores then read the same target tail and wait at the real publish gate.
Their matching StepCompleted writes race at the same next logical index.

The intact baseline accepts one winner and rejects one stale writer, then completes
and audits all28 invocations with immutable prefix and a higher terminal epoch.
The final baseline passes in36.40s; its race path passes in36.53s (37.55s package).
The production CAS-header-removal mutant acknowledges two distinct sequences at
index2. Both actual retained raw messages are logged, and the raw-state checker
rejects the target with `index2 at position3`. Detection occurs in14.01s before
workers attempt to recover the deliberately corrupt target. The mutant does not
claim a completed cohort; no acknowledged duplicate is deleted or patched away.
Timeouts, unrelated errors, compilation failures and missing raw evidence cannot
satisfy the required semantic marker.

The original runner logs/report are losslessly archived and compared to original
member bytes. Independent receipt checking decodes both stored entries, verifies
same invocation/index/result and distinct sequences, and checks named baseline/
mutant execution. Fixture hashes cover shared admission/recovery, the CAS helper
and the publish gate. The recorded HEAD is a4d57ab with these fixture edits in the
worktree before commit. Earlier bootstrap/baseline iterations preceded raw-receipt
logging; the archived final runner is the version with both raw receipts.
Hosted run37004744569 at `c6793cf` is independently accepted. All four jobs
complete successfully, with nine actual semantic baseline/mutant pairs and both
negative controls per job. Recorded source hashes match that Git revision.
Its two raw CAS receipts at sequences151 and152 decode to identical
StepCompleted/index2/epoch1/result42 entries; the retained-state checker rejects
`index 2 at position 3`. Original downloads, terminal job metadata and complete
job logs are losslessly archived in [ci-pass](ci-pass/), with an independent
acceptance record and SHA256 member manifest. This advances the third mixed
source-mutation category; enqueue now has separate local evidence, while purge,
start repair and full chaos remain open.
