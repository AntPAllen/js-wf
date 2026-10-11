# Full frozen160 qualification launch — 2026-10-11

Source50e4f9bee0931ba4474b68574a08da3bfbaafe52 includes the production source-header
fix and the new seeded recovery/repair/worker family. The sparse checkout retains
all3631 required Git inputs and3666 selected paths, verified against frozen Git
before launch. Historical docs/scale media are omitted; required Go/Python/YAML
inputs, all nonhistorical files and simulation fixtures remain. Sparse patterns
are bound in checkout.json and independently re-derived by the reviewer. The
checkout is107MiB and remains clean. No existing frozen checkout was changed.

The normal unit js-wf-main160-tier1-normal-20261011-prepared.service launched with
InvocationIDcd8fc24771f84fcd9868359b78449ba7, supervisor236839 and driver236865.
normal-launch.json binds loaded ExecStart, InvocationID, process IDs, supervisor
bytes and resource properties. It requests1000 seeds/160 families/880 pins with
eight sequential disjoint package processes using the same retained binary,
GOMAXPROCS2/GOMEMLIMIT512MiB, one-core service quota,2GB MemoryMax,
per-process300m watchdog and145200s supervisor service lifetime.

## Acceptance after actual completion

```sh
python3 docs/scale/current-main160-tier1-2026-10-11/qualification-review.py normal
```

The reviewer requires actual child/supervisor and eight package exits, MainPID0,
RemainAfterExit=yes, exact loaded command/unit/resource limits, clean frozen Git,
all required source bytes, unchanged before/after hashes, retained executable,
complete compiled/AST inventories,880 pinned passes,160000 completed seed bodies
and independent complete suite reduction. Race may launch only after independently
accepted normal at this same source, and uses a separate unit/root/invocation.

The running reviewer invocation was intentionally rejected; its refusal is
retained. No normal/race qualification result is claimed at launch. The three
older-source race/scale campaigns remain live. Later main revisions, extended
10k/100k and original native fault/scale/soak/retention/import/admission/rollout
requirements remain separate. Public continuation admission/import/online
collection remain disabled.
