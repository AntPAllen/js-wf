# Original 24-hour journal-leader soak: failed checkpoint 1750

Executed clean `e278ffbb8635042b118ed4956e0334647b2b3276`, original R5/seed1/24h,
normal 2GiB/GOMAXPROCS=2, explicit route seeds, production two-minute sync,
streaming-state audit with byte-bounded delivery. Audit limits remain 20 seconds
per attempt, three attempts and 60 seconds overall. Named test FAIL after
13096.14 seconds; producer exits 1 after archival. This is not a 24-hour pass.

Last successful checkpoint 1740 completes at 05:48:52.935090677 UTC with
48,720 invocations/journals/terminals and 537,620 entries (19.162 seconds).
Checkpoint 1750 runs 05:50:04.654620690–05:51:04.657382777 UTC, cutoff 49,000.
All three attempts exhaust their deadlines. Each reports 49,000 invocations and
zero reduced journals/entries/terminals. The source populates journal report
counts only after its whole journal scan and subsequent reduction; these zeroes
DO NOT show that no journal bytes arrived. The first attempt creates WF_JRN's
consumer at 05:50:06.565226418 and reaches deletion at its expired deadline.
The later attempts also show transient consumer-creation timeouts and retries.

The retained wrapper observes Fetch but the adopted byte reader calls Messages
and Next, which it does not wrap. Thus the trace cannot distinguish a delivery
stall from scan/processing throughput. Source control flow localizes the first
attempt to journal scanning, before the state snapshot. There is no captured
parent goroutine dump for this older source. Server-side cause remains unconfirmed.

Concurrent load: the fresh automatic-membership diagnostic starts its producer
at 05:48:43.937779400, overlaps the last successful audit and failed checkpoint,
and starts its workload around 05:50:21. Both campaigns perform their own
journal faults during this interval; the million-timer candidate is also live.
Overlap is recorded, not established as the cause. No unchanged soak restart.

## Preservation

All 6,976 canonical members were independently streamed and hashed against the
producer ledger, then matched to every still-preserved physical original.
Source-before and source-after inventories are identical. Complete producer
archive SHA256 `4f93d17e265716229ffe335e9c402b0aefb647fe809318aed826a8e5b5c6a42e`
is split into 25MiB parts here; every part was read back and its aggregate checked.
It includes actual SDK/server copies, build/process metadata, selected source,
all raw events/checkpoint traces and original closed native stores. The original
root and canonical archive remain at the path in archive-verification.json.
No stores were reopened or repaired. Launch source/executable identity checks
are documented in ../24h-launch. Preserve all failed originals.

Next diagnostic work: observe byte-reader delivery and per-scan progress under
unchanged audit budgets, then separate throughput from transport interruption.
Full matrices, actual24h and million physical-drain gates remain open.
