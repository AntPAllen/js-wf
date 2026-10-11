# Every retained checkpoint checked at its history prefix — 2026-10-11

The raw auditor now visits every completed checkpoint request, including older
archived frames and completions whose cursor pointer has not been published.
Each frame binds to that exact request/completion, invocation, SDK position,
locals hash and owned payload. Worker annotations require the state, promise,
signal and cancelled-timer sets reconstructed at that prefix, plus the original
child/signal metadata. Later history cannot repair a corrupted earlier frame.

A temporary shallow view selects this completed frame for existing independent
checks; the latest published cursor pointer is preserved and must match its
completed history exactly. No production frame reader/restorer/SDK replay is
called. Frames are loaded and checked one at a time. GraphJournalReport.Checkpoints
counts completed frames actually verified and is separate from entries/journals.

## Physical corruption fixtures

Two JSON/protobuf positives contain a state write42, first annotated checkpoint,
state write43 and later valid checkpoint, with the first seven entries archived.
Six older-frame corruptions per encoding change state, anchor, locals, cursor,
cancelled timers or metadata while keeping the latest checkpoint valid. Every
fixture first passes the independent complete physical reference audit. Updated
frame/completion/metadata hashes and all owned graph references are consistent;
these twelve rejections establish semantic history auditing, not merely hash
mismatch detection. Valid fixtures require exactly two checked checkpoints.

Focused race1.141s passes all fourteen cases. Existing state, pointer, metadata
and four native unpublished/published/archived checkpoint controls pass18.622s.
Exact process exits/logs and final native SDK timings are retained in review.json.

Native SDK controls cover R1/R3Domain normal flows, buffered signals after archive
collection, child promises and buffered child promises. Each complete audit must
report two parent checkpoint frames; retired child projections retain their
separate report scope. Actual race executable receipts and captured production/
module/worker-test source hashes are retained. This is focused development, not
clean frozen current-main complete qualification.

CI requires every new physical fixture and eight checkpoint-history receipts in
its full SDK job. Only the actually executed fixture/native subsets are claimed;
the full hosted CI job is not inferred from these local runs.

## Scope still open

Unannotated journal-only frames retain structural scope and can supply state
without SDK operations. An ambiguous ordinary signal/promise wire cannot prove
missing cache entries without workflow replay. Timer due/clock/case priority,
original child-source history, full schema, protected reader application replay,
orphan projections and original complete simulation/scale/fault/soak/rollout
gates remain open. Retired projection-only invocations have no retained frames
to audit. Public admission/import/collection remain disabled. Both larger frozen
campaigns predate this auditor change and do not qualify it.
