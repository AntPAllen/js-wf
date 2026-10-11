# Reader release with one authority observation

The full canonical failure isolated Start cleanup, previously doing two quorum
authority observations before attempting the reader-release CAS. `GraphView.Close`
now uses `ReleaseReaderCurrent`: one validated observation, verification of the
captured pin, and CAS at that exact observed head. The legacy expected-head API
shares the same pin-removal implementation. No caller-supplied root is admitted.
Exact original-head readback is unchanged; an overtaken commit remains uncertain.
The three-second Start cleanup budget and sixteen definite-CAS-rejection limit
are unchanged. An observation conflict returns immediately, preserving the old
policy. Typed phase errors retain error text and `errors.Is`; Start reports whether
authority observation or pin release failed. Pin release includes its existing
exact readback and does not separately identify the underlying native RPC.

Nine protocol controls cover healthy release, observation deadline, precommit
failure, committed lost acknowledgement, unavailable readback, overtaken commit,
definite conflict, forged reader and revoked reader. Seven view controls verify
one observation for healthy release, no observation-conflict retry, fresh
observation after definite CAS rejection, no uncertain mutation retry, exact
readback resolution, unavailable readback and overtaking. The existing seven
Start read/cleanup controls also pass. A closed view does no repeated authority
work. These tests establish operation count and fencing, not a native latency
improvement or the original full-scale failure's cause.

## Pinned trace migration

All 163 families and 946 pin paths remain. All existing workload runner source
and assertions are unchanged. Removing a transport operation changes recorded
traces: 380 files update. Of these, 374 preserve every scheduler decision, remove
2369 duplicate observations in total, and otherwise preserve transport events
except physical catalogue counters in 56 traces. The model advances its catalogue
on observations; reduced high-water/next-root counters track the removed reads.
Six cooperative signal actor traces require new actor schedules because the
deleted operation previously yielded. Their original seed, workload, explicit
fault/clock decisions, budgets and disabled actors remain. All existing scenario
invariants pass and every resulting trace is checked by exact replay.

`migrate_test.go.txt` is an explicit maintenance overlay, not a production replay
exception. It validates the entire inventory before writing pins. Normal replay
keeps exact decision/transport equality. `review.py` independently verifies old
Git bytes against `7eef1c0`, all new pin bytes, metadata/control preservation and
the duplicate/counter changes. The old traces remain available in Git and frozen
qualification checkouts. Migration does not qualify extended seed campaigns.

Development failures are retained: initial expected corpus divergence, an
incorrect event-name check, partial migrations restored to verified original
bytes, and cooperative schedule/catalogue-counter divergence. In particular,
the pinned contention test caught an intermediate observation-conflict retry
introduced during the refactor; typed phase handling corrected it before final
qualification. The early commands/maintenance helper did not capture their test
executables; their logs and the final helper are diagnostic evidence. Final SDK
execution receipts below do capture actual executable bytes and exits.

## Final checks and remaining native failure

| Check | Actual exit | Scope |
| --- | --- | --- |
| Protocol/view/Start controls and all pins, normal | 0 | 1003 named PASS results |
| Same checks, race | 0 | 1003 named PASS results |
| Canonical R3 active10/10/1, normal | 0 | Full diagnostic/raw audit, package18.268s |
| Canonical R3 active10/10/1, race | 1 | Signal publication append unknown, package69.711s |

The native race failure occurs after ten handlers are active and purge/reuse
overlap has completed, during fresh signal publication:
`signal publish outcome unknown: journal append outcome unknown: context deadline exceeded`.
There is no reader-release error in that run. Its remaining result/raw audit was
not reached. This result does not establish a regression cause or a server cause;
canonical native signal deadlines were already open in the implementation plan.
No failed race or full-scale acceptance is promoted, and no bounds were extended.

Independent review binds 3846 Go/Python/YAML/module/pin inputs, six actual SDK
executables/exits and both closed native censuses. Normal native has2161 files /
12,358,886 bytes; failed race has2166 files / 14,546,396 bytes. Retained binaries
and stores are under `/home/exedev/js-wf-reader-release-current-20261011`.
CI requires all23 phase/release controls in normal/race modes; all14 embedded
Python blocks compile locally. No hosted completion is claimed. Latest-source
full seed qualification, native race diagnostics, full10k scale, fault/soak/
adoption and other original release requirements remain open.
