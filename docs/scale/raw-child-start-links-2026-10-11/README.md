# Retained child start linkage — 2026-10-11

The independent raw journal auditor compares each consumed child annotation with
an available matching-generation child's canonical start metadata. Parent type,
ID, generation and return signal must match the preceding parent call; the child
input hash must match the call input hash. Validated cursors include retired
children. No production graph reader or start validator supplies this comparison.

`ChildStartLinksVerified` counts matched retained witnesses;
`ChildStartLinksUnavailable` counts absent, legacy or different-generation
witnesses. A later child generation does not establish an old return's origin.
Missing witnesses do not fail otherwise valid local history, and are not counted
as verified causality. Existing outcome and owned-body checks still apply.

## Evidence

- Baseline f7923fb plus retained baseline fixture: actual exit1 because the old
  auditor accepted child start parent generation2 for a parent generation1.
  See baseline.log, baseline-source.txt, baseline-fixture.go.txt and command JSON.
  The baseline executable was not captured.
- New physical controls: JSON/protobuf × nine cases =18, six positive and12
  negative. Real GraphStore writes create a retired child start; independent
  physical reference auditing passes before every semantic check. Parent type,
  ID, generation, signal, missing parent and input mismatch are rejected.
  Healthy, unavailable and reused-generation evidence counts are asserted.
- Broad race command: actual exit0, Go test47.467s (driver63.597s), all38 raw graph
  top-level tests and four native checkpoint/census cases. The earlier focused
  command also passed before counter assertions were added; its executable
  was not captured. Final broad/native executables were captured live with hash,
  command and build metadata.
- Native race command: actual exit0, Go test77.967s (driver94.849s). R1/R3 child
  and buffered-child flows independently audit four matching child start links,
  zero unavailable links, eight checkpoint frames and four retired projections.
  Native stores are ephemeral; this is not restoration or storage power proof.
- 1131 source/module/CI inputs stayed unchanged through completion. The manifest
  was captured after launching the broad race driver and before launching the
  native driver; it is not claimed as a prelaunch broad source capture.
- `python3 docs/scale/raw-child-start-links-2026-10-11/review.py` verifies receipts,
  unchanged hashes, native counters and executes the exact new workflow guard
  against race.log. Workflow YAML parses; hosted CI is not claimed.

Full original publication/parent-source/lease causal history, old generations
without retained witnesses, child result equivalence to the original terminal,
client linearizability, all-peer durability and original scale/fault/soak gates
remain open. Four frozen long campaigns predate this code; no current161/894
complete normal/race/extended qualification is claimed.
