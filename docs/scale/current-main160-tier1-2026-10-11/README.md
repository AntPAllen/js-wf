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

## Frozen normal accepted after actual completion

Independent reviewer accepted the complete normal1000 run at source50e4f9b:
160 families/160000 completed bodies,880 exact pinned replays,228 top-level
passes, eight actual disjoint package exits0. All3631 Git inputs match frozen
source and remain unchanged. The retained binary SHA is
5b874a90f4c8b65ecc2480db2847fffa39c61ab3fcd75c5089f5e83ce142f8ab.
Supervisor/driver exits0 and loaded InvocationIDcd8fc24771f84fcd9868359b78449ba7
are proven with MainPID0/RemainAfterExityes. Finished2026-10-11T04:08:55.633892Z.

normal-terminal/ retains all nonbinary stage files; large logs/source manifests
are losslessly compressed. normal-terminal-files.json binds original bytes and
stored files; every copied file was read back and compared. The original binary,
frozen checkout and loaded unit remain local and required by the independent
reviewer. This proof does not cover newer main,161-family qualification, race,
extended or original native/scale/fault/soak/rollout acceptance.

## Same-source full race launched after accepted normal

Unitjs-wf-main160-tier1-race-20261011-prepared.service started04:10:38UTC,
InvocationIDadb2b689836b43ba9d16e55d1e4476a8, supervisor244649/driver244675.
race-launch.json and race-loaded-unit.txt bind the actual loaded command,
resources, process handles, invocation and unchanged supervisor hash. It uses
the same source50e4f9b,1000 seeds/160 families/880 pins, eight sequential disjoint
processes and retained race binary, one-core quota/2GB memory, GOMAXPROCS2/
GOMEMLIMIT512MiB,300m per-process watchdog and145200s unit lifetime.

The reviewer refused this actual running stage with exit1; the refusal is retained.
Launch is not race acceptance. The live race-state.json remains untracked until
actual completion. After closure run the existing qualification-review.py race
command; do not change the captured supervisor or frozen source.
