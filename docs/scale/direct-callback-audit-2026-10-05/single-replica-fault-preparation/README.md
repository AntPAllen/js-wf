# Direct R1 native cursor-loss qualification preparation

Select `TestStreamingAuditNativeJournalFaults/direct-single-replica` under the
actual isolated race SDK. Fixture retains complete1500 invocation/journal/
terminal and6000-entry oracle, with24MiB payload crossing8MiB client refill and
4096-record windows. Source streams remain R3; only temporary INV/JRN audit
consumers requestR1. Server-returned cached config must match requestedR1.

Three independent fresh fixtures: consumer-owner library shutdown identified
from actual ConsumerInfo.Cluster.Leader at128 accepted journal records;
explicitly acknowledged DeleteConsumer for the captured cursor at128; and
cancellation at128. Owner/deletion targets require correct actual name/stream,
MemoryStorage/AckNone/R1 and positive server-side pending tail. Owner lookup
failure is a failed qualification, never a substitute stream-leader injection.

Each loss case must return the complete report with exactly6000 accepted journal
visits within original20s. Cancellation must return context.Canceled and exactly
128 visits. Both audit stream consumer counts must be zero after cleanup. Current
replay/transport/error/two-resume rules are unchanged; no recovery behavior is
broadened before evidence. Native failures will be preserved and reviewed.

Race preparation batch/byte/callback/direct/contract controls pass1.037s. This
compiles the new native mode but does not execute its opt-in faults. Producer
selects four-core/GOGC500/4GiB race profile and captures the exact SDK/source;
reviewer accepts evidence preservation of either native verdict and qualifies
only allthree named case passes. Linked currentR3 servers have no separate
external executable claim. No OS SIGKILL/R5/full400k fault/default/24h acceptance.

Previous quiet-capacity978-file copy reclaimed after complete pushed canonical
base-plus-delta/current-copy hashes, closed SDK/five server PIDs and all visible
task descriptor checks. Originals/source/executables/canonical evidence retained.
