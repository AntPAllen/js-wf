# Strict checkpoint promise outcome envelopes

Checkpoint Frame.validate previously decoded each PromiseOutcomes value with a
permissive local JSON decoder. A matching outer frame checksum did not prevent
nested duplicate/alias/null metadata from being admitted into restored SDK state.

The runtime outcome envelope rules now live in internal/checkpoint's
DecodeOutcomeEnvelope. wf.DecodeOutcome maps its errors to ErrCorruptJournal;
checkpoint validation uses the same rules with a typed limit-entry schema and
retains existing result reference checks. Encoded user results, continuation data,
limit request contents and limit payload contents remain opaque. Absent legacy
optional identity remains valid; generation and terminal-kind checks remain with
callers. This change does not add child identity/program equivalence semantics.

## Evidence

- Baseline source 7e70a60, actual exit 1: 17 malformed nested outcomes passed frame
  Decode, frame Encode and NewCheckpointContext. Each fixture is valid JSON with
  a matching SHA-256 and otherwise valid identity/anchor/state. Nine positive
  controls pass; the preexisting unknown-field rejection also passes.
- 27 controls test Decode/Encode plus public SDK restoration. Nine positive cases
  preserve exact frame bytes/data and restore state (including omitted identity,
  inline/external results, encoded null/duplicates and opaque rejected payloads).
  Eighteen negative cases reject before returning frame or SDK state. Restoration
  performs zero appends; a subsequent valid GetState operation is checked separately.
- Final exact terminal CI commands: normal/race actual exit 0, all 141 named
  metadata/result/export/checkpoint controls plus selected replay/retention tests.
  Exact guards are extracted from parsed current YAML and applied to both logs.
  Hosted CI is unclaimed.
- Full wf and internal/checkpoint race suites pass, respectively 163.933 s and
  4.536 s. The command has no test selector; existing frame roundtrip/opaque locals
  and uninterrupted/restored continuation controls pass alongside the new cases.
- Native race child and buffered-child R1/R3Domain flows pass in 150.486 s with
  eight checkpoint frames and four verified start/terminal child-link pairs.
- All 1157 prelaunch source/module/workflow inputs remain unchanged. Eight actual
  executables were captured by SHA/build/Go parent PID/timestamp across final
  commands. The short normal checkpoint executable was missed and is explicitly
  unclaimed; its actual process command exit and log are retained.
- Failed initial fixture assertions are retained: the first fixture incorrectly
  counted a subsequent GetState's two appends as restoration appends. Initial
  helper compile failure retained a redundant package qualifier after extraction;
  its actual exit 1, original source and manifest are preserved. Reviewer verifies
  the exact qualifier correction and the final baseline's three admission witnesses.

Run `python3 docs/scale/checkpoint-promise-outcome-admission-2026-10-11/review.py`.

## Scope

Matching checksums distinguish this admission gap from ordinary byte corruption.
Native evidence covers healthy carried child outcomes. It does not establish all
native negative faults, promise/child ownership and generation equivalence, full
causal replay, or all physical adoption/maintenance paths. The frozen full162
normal campaign predates this fix. Full current qualification and original release
gates remain open. Public continuation admission remains off.
