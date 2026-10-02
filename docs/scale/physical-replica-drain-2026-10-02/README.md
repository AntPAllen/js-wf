# Physical replica drain contract

The timer-volume runner now retains local monitoring replies from all three
physical replicas on each final drain attempt. The original shared3s inspection
budget and delivery/campaign limits remain unchanged. Zero leader messages and
zero durable pending counts must be accompanied by an independently parsed
empty local WF_RUN on every replica, all64 named consumers with zero pending/
ack-pending, distinct server identities and matching nonzero final sequences.
Missing counts, consumers, replies or identities cannot stand for empty state.
Raw responses and controller observation times are synced in
`physical-drain-audits.jsonl` and the latest audit is retained in the report.

Release verification requires both logical and physical drain proof. Removing
either proof from an otherwise valid million-receipt report is rejected. Legacy
short smoke reports retain their old verification scope; a supplied physical
proof must validate even for smoke. The original failed million verdict remains
failed. This contract does not resolve the source/index persistence inconsistency.

The permanent runner SHA256-checks its three archived monitoring fixtures before
using them. The full package passes under race, including the real retained-store
snapshot test (768/141/0 local messages), cancellation/partial evidence cases,
false-empty cases and actual receipt-process-kill parent. Only its child helper
skips at top level. Two actual compiled controls fail semantically: ignoring local
messages falsely certifies the retained snapshots; removing mandatory release
proof admits a report with its physical audit stripped. Exact overlays/events,
source hashes and all input snapshots are archived losslessly. Every archive
member is compared by SHA256 on readback. Worker-volume vet passes.

The initial control attempt removed the only read of a local variable and failed
compilation; it was rejected and remains under the earlier temporary evidence
root. The corrected final control reads the variable with an impossible bound;
its actual named failure is semantic. Final source hashes identify tested changes
on parent4f610b9 and the corresponding files in this commit. The dedicated manual
workflow repeats the final contract. A live new-source smoke and fresh million/
24-hour release qualification remain required; no old campaign is reclassified.
