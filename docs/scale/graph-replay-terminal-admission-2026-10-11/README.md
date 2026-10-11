# Canonical replay export terminal admission

Inspection found a permissive intermediate json.Unmarshal in replay export.
The actual baseline disproved an admission defect: export invokes the strict
ValidateReplayGraphHistory boundary before returning a snapshot. Production
export and validator sources are unchanged. New regression coverage directly
exercises that public export contract and records its downstream dependency.

## Evidence

- Source f2c87c6, actual normal baseline exit 0: 42 controls over physically
  consistent canonical JSON/protobuf-v1 two-record journals. Eight valid cases
  preserve original terminal payload and input; 34 duplicate, alias, escaped-key,
  unknown and null metadata cases return ErrCorruptJournal and an entirely empty
  snapshot. User result bytes remain opaque. Graph physical references and the
  unchanged journal are checked before/after export.
- A Go source overlay disables only the final ValidateReplayGraphHistory call.
  Actual exit 1: all 34 malformed exports fail semantic assertions, while eight
  valid controls still pass. No production file was mutated. Exact source,
  overlay, logs and actual exit receipts are retained. This is a focused boundary
  dependency test, not the original full six-invariant mutation campaign.
- Final exact terminal CI commands pass normal/race, with 114 metadata/result/
  export controls (72 prior and 42 new) plus the existing selected replay and
  parent-retention tests. Exact guard is extracted from parsed current YAML and
  run on retained logs. Hosted CI is not claimed.
- 1155 source/module/workflow inputs match prelaunch hashes after completion.
  Actual wf and worker executables were observed for both final commands and
  bound by SHA/build metadata, Go driver parent PID and observation timestamps.
- The first reviewer used a nonexistent legacy filename for the strict decoder;
  that actual exit 1 and original script are retained. The corrected reviewer
  verifies wf/replay_record_order.go directly against f2c87c6.

Run `python3 docs/scale/graph-replay-terminal-admission-2026-10-11/review.py`.

## Scope

This proves public terminal-envelope export admission and no partial snapshot on
rejection. It does not establish all checkpointed promise envelopes, graph journal
causality, result/program equivalence, native negative fault histories, or full
current qualification. Checkpoint promise envelope validation remains a separate
item: its local decoder differs from the runtime outcome decoder. Original release
gates remain open, and public continuation admission remains off.
