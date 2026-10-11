# Worker epoch ownership admission — 2026-10-11

Worker.execute rejects retained records that declare different nonempty worker
IDs for the same nonzero epoch, before handler admission, terminal repair and
signal draining on that execution path. Omitted IDs provide no evidence and do
not erase a previously named owner. Epoch-zero histories remain compatible.
This matches existing offline replay/integrity ownership semantics; it does not
prove the lease authority or the original cause of historical NATS lease faults.

## Worker tests

TestWorkerEpochOwnerBeforeHandler uses the production worker dispatch path with
legacy and canonical in-memory transports. Eleven cases per backend cover:

- Six valid: one owner, successor owner, same owner with a new epoch, an omitted
  middle ID, all IDs omitted, and legacy epoch-zero IDs.
- Five conflicts: different request/completion owners, a conflict separated by
  an omitted ID, no named Started owner, and a conflicting successor epoch.

The existing metadata fixture was extracted into a shared helper, preserving its
60 controls. Epoch tests prime and release two actual modeled leases so the new
worker's epoch succeeds the retained named epochs. Positive controls enter one
handler and replay without SDK callbacks; negatives require zero handlers/effects
and unchanged journal length. Canonical reference checks remain enabled.

Baseline8c22778 plus final tests exits1: all ten ownership conflicts enter the
handler; all12 positive histories pass. Original worker source and actual log/
command result are retained. Baseline executable observation was missed.

Final normal/race commands cover all82 controls and pass exact current workflow
assertions. Three final normal/race/native executable hashes, build metadata and
commands were captured. Review checks all prelaunch Go/module/workflow inputs
unchanged. Hosted CI execution and full current seeded qualification are open.

Native R1/R3 child/buffered-child continuation compatibility runs with independent
raw SDK auditing, requiring four start/terminal witness pairs and eight
checkpoint frames. Its stores are ephemeral; this is not native corrupt-history
injection, full worker suite qualification, fixture restoration or power-loss
proof.

## Rejected observer receipt

The first successful normal command's observer captured the concurrent race
executable (observer-mismatch-normal-*). This binding is not accepted. Observer
selection now requires the worker process's parent PID to match its actual Go
driver. The final normal command was repeated to resolve this concrete provenance
failure; its correct executable parent and non-race build are independently
checked. The race command's observed executable has the expected race build.

## Review

```sh
python3 docs/scale/worker-epoch-owner-admission-2026-10-11/review.py
```

Corpus remains162 families/922 pins. Five older campaigns were revalidated active
and predate this fix. This checks contradictory evidence in retained admitted
records; missing IDs, compacted prefix ownership, protected-reader histories,
writer authority and full I1–I6 proof remain separate. Complete program/causal
semantics, original fault/scale/soak/power/client/rollout requirements remain open.
