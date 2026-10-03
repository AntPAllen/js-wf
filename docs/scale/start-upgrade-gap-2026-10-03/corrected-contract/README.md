# Actual Start crash carried across a retained-store upgrade

Clean sourcecc0fcd189880c561cb0ba7a90a0dde581586bb46 passes both R3 mixed-version
profiles under race instrumentation in40.509s package. Production Client.Start
executes in a real child connected directly to2.11.17. Its first invocation
publish returns, the next dispatch boundary writes a marker without publishing,
and the parent verifies actual SIGKILL status. The retained invocation exists;
no matching run message or journal exists.

The old peer is then upgraded on the same store. The pending invocation's full
retained record is unchanged. Production StartScan repairs it through the
upgraded connection; worker completion and matching duplicate Start preserve
the original invocation sequence. Confirmed-kill-to-verified-terminal times
are12.424619138s and9.782391108s, both within the explicit30s bound. Earlier
mixed-version fallback/manual-repair checks and the final four-invocation raw
integrity audit still execute.

The compiled control suppresses only the gap invocation's repair publication.
It executes through the same upgrade and fails in18.013s at the required
`post-upgrade process-gap repair produced no dispatch or journal` assertion.
It does not fail at Await, a build, a skip or a global timeout.

Independent review verifies all715 before/after source hashes against Git,
both retained race binaries/build metadata, precise overlay derivation, named
and package verdicts, raw process markers, invocation equality, terminal bytes
and reported recovery bounds. All1293 originals are retained across four tar
parts; extract each into the same directory to restore the evidence tree.
Every member SHA256 is checked by reopening each temporary archive before
atomic rename. Commands, source, binary, original stores and review utility
are preserved.

This qualifies the R3 mixed-version per-phase process-crash contract. Integrating
the same boundary into sustained R5 mixed rolling faults remains required;
this does not clear the full200-seed matrix or24-hour soak.
