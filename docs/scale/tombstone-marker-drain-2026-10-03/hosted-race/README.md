# Complete marker-cleanup runtime graph: hosted race

[Run 37146525331](https://github.com/AntPAllen/js-wf/actions/runs/37146525331)
completed successfully at exact source
`9ecc37c74979132a8a2bae5877eb44bc175cbf89`.
Public artifact `11283830949`, `tier1-race-evidence`, is 15,203,420 bytes.

Independent review accepts:

- 175 top-level tests and two documented trace-only skips;
- all 391 pinned regressions;
- all 121 scalable workloads, contiguous seeds 1–1,000 (121,000 bodies);
- 1,140 unchanged selected before/after source hashes, identical to exact Git;
- actual retained race executable SHA256
  `0cda8018c064439e2c680d4932cca2184b20e84a2c3d142ebe4752ac0f2f5078`;
- executable build settings, compiled inventory and exact-source AST inventory;
- byte-identical report regeneration from complete raw events and their hash.

Package elapsed: 2,149.903 s (producer wall time 2,150.26 s). Aggregate counters,
including fixed/repeated tests: 124,847 schedules, 1,807,121 choices and
27,606,589 transport events. The new marker-drain model retains its 128 fixed
seeds/replays; it is not counted among the scalable 100k workloads.

The executable's download permissions were restored for inspection without
changing any byte. Every archive member was reopened and SHA256 verified before
atomic publication. Metadata binds the exact successful run/job/artifact/source.

`runtime-equivalence.json` separately compares every tracked non-test Go source,
module declaration, complete simulator source/pin file and Tier1 producer/suite
verifier against observed main `fd92ebc`. All compared bytes are identical to
this tested source. It does not claim that later integration-test or other
verification-script edits were present in this executable or qualified by it.

This completes the race suite containing all six scanner-prefix corrections and
physical deletion-marker cleanup. Normal 100k, latest real full matrix, sustained
mutations, population-independent latency and 24-hour qualification remain open.
