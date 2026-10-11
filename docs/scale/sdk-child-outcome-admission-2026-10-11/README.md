# SDK child outcome admission — 2026-10-11

Call, AwaitPromise and Select now validate child outcome envelopes before consuming
a signal or advancing a replay completion. Select also invokes the configured child
provenance validator. Invalid metadata, contradictory failed results, and malformed
external result references cannot populate the promise cache. Checkpoint admission
rejects failed outcomes carrying inline or external results. Legacy omitted identity
and opaque user result contents remain supported.

## Regression evidence

The final fixture contains 204 SDK metadata controls (54 positive, 150 negative)
and six provenance denial controls. The three-file baseline overlay from 4fd7485
fails all 150 negative metadata cases and four denial cases; two Select cases bypass
the provenance hook. The checkpoint frame baseline admits both new contradictory
failed-result cases through decode, encode and public SDK restoration.

Normal and race CI selections pass all 353 named controls. Full SDK and checkpoint
race suites pass, including repeated-await copying and external-result read retries.
`review.py` verifies 1158 unchanged Go/module/CI inputs and eight captured actual
executables. The short normal checkpoint executable was missed by observation;
its command exit and tests are recorded without claiming an executable capture.
Initial 31-case and exploratory fixed logs are retained separately; final acceptance
uses the structured command receipts and the 34-case fixture.

## Native compatibility remains unqualified

The eight native promise-priority variants completed with actual exit 1: six pass,
including all four failed-child variants and both unarchived successful-child
variants. Those six verify 12 checkpoint frames and six child start/terminal witness
pairs. Both archived successful-child variants (R1 and R3Domain) fail with
`journal append outcome unknown: context deadline exceeded`. Their enclosing
120-second contexts still had about 58 seconds remaining. No data race was reported.
The operation observer was not enabled for these successful-child variants, and
native stores were not retained by this command. The cause is unconfirmed.

`review.json` therefore reports regression evidence verified, but acceptance and
native compatibility false. No native retry or timeout relaxation is claimed.
Further work must explain uncertain append recovery and obtain native compatibility
for the final source. Frozen full162 qualification predates these changes. Public
continuation admission remains disabled; identity equivalence, causal replay and
the original release gates remain open.
