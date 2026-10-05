# R5 cursor process restart replay diagnostic

Fresh fixtures preserve original1500/6000 cardinality and20s deadline. Consumer
Info snapshots capture creation time/config/owner and delivery/ack positions at
creation, fault targeting and replay-proof queries. Existing strict replay
admission remains unchanged, so a rejection remains a failed recovery.

Cleanup now uses one-second stream-info requests, retrying typed transport or
request deadline failures only inside the same original20s audit context.
Every count/error/time is retained. The previous five-minute missing reply is
preserved, not promoted. Compile/opt-in skip passes; native execution pending.
