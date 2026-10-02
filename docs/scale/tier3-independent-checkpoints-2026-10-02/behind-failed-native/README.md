# Independent checkpoints pass; native row remains failed

Run36969577235 at6e34939459a8ac898781e805c7de0c87bccb13a3 fails after375.556s.
The independent audits complete at batches10/20/30, covering280/560/840
invocations, matching journals and terminals, and3078/6160/9250 entries.
Batch30 audit finishes05:44:47.275727647; batch31 finishes05:44:49.729 and
batch32 finishes05:44:57.391. Fault10 admission then expires05:45:08.236.
Therefore this failure does not overlap an active checkpoint audit. It does not
invalidate the component audits or pass the final native row.

Role receipts put WF_RUN and WF_JRN on shifted node4 at05:44:58.125/58.188.
Batch32's first two-second timers were requested around05:44:57.51, with healthy
physical clock hints. Later correctly shifted hints belong to250ms waits,
shorter than the admission's750ms removal lead. These observations suggest an
admission opportunity/workload timing issue; the complete native/repair/lease
causal join remains required before changing the workload or blaming NATS.
No unchanged rerun was launched. There are nine cut artifacts; the tenth fault
is not claimed successful. There is no accepted final audit, latency/history or
physical-drain result. Original uploaded artifacts are archived without rewriting
failure evidence, alongside terminal metadata and failed-step logs.
