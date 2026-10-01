# Mixed seed 3 queue metadata lookup timeout — October 1, 2026

CI at a36cb7e (run 36812809142) passed seeds 1 and 2. Seed 3 returned all 28
terminal outcomes with terminal p99 11.638 seconds, then consumed its four-minute
context in the final restarted-node WF_RUN Stream lookup. The final integrity
loop had passed before that lookup. This is a failed gate, not a clean queue-drain
check, despite the later independent monitoring observations.

The retained schedule delays node 1 by 35 ms, isolates/kills node 0 and pauses
node 2. Monitoring added in a36cb7e captured before, after heal, during recovery
and final state. Final responses from all three nodes report WF_RUN messages=0,
last_seq=133, first_seq=134 and leader node 2. The leader reports both followers
current; follower reports at that instant list one peer as not current. The
metadata lookup was through the freshly restarted node 0 client (third).

This separates the failed client metadata read from evidence of retained queue
contents. It does not establish whether the request, response, routing or server
processing caused the timeout. A bounded/retried read contract is the next
concrete investigation: the existing lookup uses the whole workflow context
outside the later 30-second drain loop. The read can hang before that loop.

Raw CI log, fault schedule, client operation events, node/disk logs and all
monitoring phases are retained. Source was a36cb7edd7e5804f4df4d80f0d491d38abafb4c4.
No runtime fence, latency or drain gate is weakened. This miss remains open
alongside mixed latency seeds 2/12/65 and final-source full matrix/24-hour soak.
