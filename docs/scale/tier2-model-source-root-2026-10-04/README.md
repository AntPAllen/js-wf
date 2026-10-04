# Source-isolated Tier2 journal history-model review

The snapshot timeout diagnostic changed production `journal/snapshot.go` on
main. The Tier2 campaign at `c4fed061bc614488d4f89b53b216b756490f7da0` must
still be reviewed using its exact actual model inputs. The reviewer now accepts
`--model-root` for an isolated source checkout. This changes the build directory,
not its acceptance rules: the complete actual `go list -deps` local dependency
graph plus go.mod/go.sum must match executed Git before compilation and after
history review. All raw binding/inventory/fault/latency/model checks remain.
The report records the actual model source root.

An isolated clean c4fed06 worktree with required source directories re-reviews
the already accepted complete journal **seeds 1–12**: **31,472 invocations,
346,713 entries and 228 faults**. All three production history models pass,
with all **45** dependency inputs matching executed source. Complete raw fault,
latency and model output regenerate. This creates no new seed coverage and
never promotes the parent, full row, full matrix or 24-hour soak.

Three actual raw-shard controls reject before producing an output report:
current main as the default build root; current main as an explicit root; and
one changed dependency byte in the otherwise correct isolated checkout. The
isolated file is restored and all 45 hashes reverify. All three existing unit
controls pass. Initial sparse setup attempts fail on missing go.mod/protocol
inputs and are preserved; adding explicit cone mode and the protocol directory
allows the complete model dependency graph to compile. No initial attempt
creates a qualification report.

`proof.tar.gz` retains final independent report, all per-seed model verdicts,
source/root/hash records, positive and rejection logs, reviewer/helper/unit
sources and setup commands. Every member was independently read back and hash
verified. The helper binary is temporary and not retained by this tool; no
original workload executable or broker stores are claimed. Full original raw
inputs remain in the earlier [journal1–12 canonical proof](../current-tier2-matrix-2026-10-03/journal-1-12/).
That large archive may be absent from the sparse local worktree; restore its
recorded bytes from Git before reviews requiring that path. The current RAM
raw input directory is unchanged.

Use `--model-root /tmp/js-wf-tier2-model-c4fed06` when reviewing the remaining
journal shards from the same campaign. Source hashes are mandatory even when
the isolated checkout's HEAD appears correct. Existing live campaign jobs
are not restarted due to the later main-source diagnostic correction.
