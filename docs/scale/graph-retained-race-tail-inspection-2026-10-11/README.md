# Retained race tail inspection and bounded challenge — 2026-10-11

The original frozen 9189259 100,000-entry race failed at an uncertain append to
index 40,959. All 866 closed native files (685,671,380 bytes) were hash-verified,
copied, and the copy was verified against the same census before reopening.
Only the copy was opened. Public NATS and graph APIs supplied the observations;
no private file-store parsing or modification of original evidence was used.

## Durable boundary found

The reopened copy confirms invocation 1 and owned canonical input `null`, with
journal count/tail 40,959, StepCompleted at index 40,958, no checkpoint pointer,
and no record at index 40,959. Eight suffix entries are four state_set request/
completion pairs for padding values 20,474 through 20,477. This determines that
the failed request is absent from the restored durable graph. It does not show
whether a transient server action occurred before the original process exited,
or explain the request deadline. The original profile's one ReadBlob deadline is
consistent with pre-publication validation failing, but cause is unconfirmed.

## One bounded challenge on the copy

A race-instrumented helper acquires a fresh lease (creation revision/epoch 42,126),
renews it, and uses the real SDK SetState to write padding value 20,478. Each of
the two graph appends is attempted once under the unchanged 15-second bound.
Index 40,959 is a 13-merge carry boundary producing a 14-node append spine;
it completes in 1.341049056 seconds with 483 traced requests. The completion at
index 40,960 takes 0.182718970 seconds with 67 requests. The inspected original
suffix remains byte-equivalent as decoded records. A subsequent cold reopen
confirms both new records, matching canonical identity and count/tail 40,961.
The reopen uses a new server process with the same OS page cache; it is not a
VM restart or storage power-loss test. This is one SDK state operation, not
execution/recovery of the full workflow.

All actual build/run commands exit 0. Both helper sources, executable digests,
race build provenance, result logs and receipts are retained. The Go/module
snapshot is explicitly post-execution and matches all 1157 Git inputs at aedc09c;
it is not presented as a pre-run source manifest or the original failed reader.
The closed modified copy has 866 files/684,183,971 bytes, with a complete new
census; normal file-store expiry and checkpointing can change physical bytes.
Original files are verified unchanged after all copied-store operations.

The original and modified copy plus both executables remain local. Re-run review.py
from the repository to verify the original census, closed modified copy, source
snapshot, receipts, eight-record suffix and cold-reopened challenge records.
The helper requires a verified-copy marker and must never be pointed at original
proof. The challenge is intentionally one-shot: it refuses a cursor other than
40,959 with no checkpoint pointer. It performs no collection or full-history audit.

Full workflow recovery, original 100,000-entry race acceptance, server/client
failure causality, canonical clock/effect histories and original release gates
remain open. No retry or timeout configuration was changed. Public continuation
admission remains off.
