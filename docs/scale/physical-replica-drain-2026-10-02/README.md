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

## Hosted contract and live smoke accepted

[Hosted contract37043119136](https://github.com/AntPAllen/js-wf/actions/runs/37043119136)
passes at exact329a56d. Uploaded source hashes match Git, and the original
positive/negative executions are preserved under `hosted-pass`. The two semantic
controls are caught, with no build failure substituted for detection.

The clean329a56d live1000-timer/60s smoke survives two actual all-server SIGKILL/
restarts healing in9.534s/9.569s. All receipts verify offline. Each raw local
reply shows zero messages, zero pending/ack-pending and64 consumers; all three
server IDs are distinct and final sequences match at2000. Raw p99/max are
13.116s/13.253s against diagnostic30s/60s bounds. This accepts the new all-replica
monitoring integration only, not the million/24h2s/30s release profile.

The first smoke used5s lead; consumer setup consumed that runway and it failed
before any publish. That setup attempt remains failed and is archived alongside
the qualified30s-lead run. No runtime recovery target was changed. `live-smoke-pass`
retains every top-level report/receipt/observation/audit/log plus build and terminal
service metadata and independent raw-reply review. Every member in both archives
was SHA256-compared after compression. Physical stores and binaries remain local
and are excluded from those published archives.

The rejected runway cause is corroborated by retained consumer metadata: first
due17:48:15.116759647Z, last consumer created17:48:15.490332750Z. All64 creation
timestamps are published separately in `live-smoke-pass/rejected-runway-cause.json`.
Thus consumer setup had already crossed the loading deadline before publishers
started; this is a setup configuration mismatch, not an all-replica drain miss.
