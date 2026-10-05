# Preserved before-manifest worker-kill failure

Source `08835e7`, retained race binary, actual three-process R3 cluster. Native test failed after 35.00 s because its assertion required a new frame after recovery. No native pass is claimed.

The child was reaped with SIGKILL before manifest publication, after its frame and StepCompleted were durable. The successor replayed the journal, repaired the manifest using that recorded frame/epoch, and completed with a higher lease epoch. The frame remained reachable, so requiring its collection was incorrect. `worker/continuation.go` preserves the completed record's anchor on replay; this is expected repair behavior. Final GC assertions were not reached.

All 651 selected repository Go/module inputs match recorded Git and before/after copies. Actual parent/child binary SHA matches. All 1,082 original archive members and both split parts read back and verify. Original stores are retained and were not independently reopened. Corrected assertions require reuse and retention of the reachable frame; budgets and effect/fencing checks remain unchanged.
