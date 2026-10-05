# Experimental compact metadata controls

Conservative ACK coordinate parser handles ordinary nine-token v1 and eleven/
twelve-token v2 forms. It preserves stream, sequence and timestamp; every numeric
field is checked. Other syntax delegates to SDK Metadata. Exact pinned SDK delivery
receiver type selects the fast path; wrappers/fault models keep Metadata overrides.
Common scanner order/gap/cutoff/recovery logic is shared. Defaults remain SDK-based;
only an explicit diagnostic scanner/profile flag selects this candidate.

20,010 generated/edge subjects differential against the pinned SDK shared parser,
zero parser allocations, and wrapped Metadata preservation verified. SDK benchmark
252.1ns/304B/2alloc; candidate155.9ns/0B/0alloc. This isolated parser comparison
is not a full audit performance result. Original compile failure omitted forwarded
timestamp; fixed before any native run, and failure log remains preserved.

Race routing/cancellation/prefetch/error/replay controls pass. Shared native full
compaction/cohort/corruption/state comparison includes candidate, plus a bound
native delivery contract that checks actual modern SDK coordinates and confirms
zero allocations. Native execution/capacity qualification pending.
