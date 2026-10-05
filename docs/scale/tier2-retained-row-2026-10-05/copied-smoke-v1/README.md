# First copied Tier2 smoke review: retained checks pass, server observation gap

Fresh copies only of the closed584aac5 journal-smoke process stores.
Actual reviewSDK458789 / SHA256
`b4d8571c74f457cb820d49f9751fe848b45deb0578a78dba5cfd30b8a380e20c`.
Helper source1300519,1695 selected production/external inputs independently bound.
All2154 originalfiles unchanged. Rechecked Start/signal/result histories, full
196 invocations/journals/terminals and2156 entries. Audit198.545ms, whole reads
4.210745s underoriginal20s. Allthreeclient queues zero and64durables pending/ack zero.
No workers, manualACK, provisioning or original-store reopen.

The v1 main-task-only observer recorded zero server processes. This limits server
executable provenance for this observation; native SDK identity and retained
checks remain supported. Existing captured processes in other runs remain valid.
The changed all-task-thread observer is verified separately on another fresh copy.
Complete closed originals/copies, actualSDK, helper/producer/observer and selected
source retained in archive parts with full member readback. Independent reviewer
and JSON are delivered beside the archive. Original35s smoke and full-matrix
qualification are unchanged.
