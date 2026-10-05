# Native lease loss while an SDK effect ignores cancellation

Executed source: `ee2f53dc55d21fcdaedafd5b09539cf6735e0368`.
Actual SDK PID180500, SHA256:
`2e163bac4e2b74750a4583fc6d13d68c8069521a29136949ea8838bd42dc5e5a`.
The native race test passed in **14.96s**, with **1.209558533s** recovery from
observed effect-context fencing. Original50s fixture context, strict30s recovery
check, observed12s lease TTL and13s AckWait remain. Production code unchanged.

The real R3 file-backed cluster committed lease revision4→5 for old owner/epoch1
while the client's renewal response was held. The retained, untruncated proxy
trace has the CAS request for revision4 and forwarded lease PubAcks1/2/3/4/6;
revision5's committed renewal PubAck is absent from forwarded traffic. The old
worker reports one fencing event. Its SDK effect ignores the cancelled context
and stays blocked while the worker loop stops and successor epoch7 completes.

The successor retries the unfinished requested step, returns42 and writes the
sole terminal outcome. Only then is the old effect released to return stale7.
All three peer journals and results must remain unchanged after that return.
Independent JSON/base64 decoding verifies the four records, request hash,
contiguous indices/sequences, epochs1/1/7/7 and terminal bytes42. Two effect
executions occur; abandoned external code can continue and needs idempotency.
Integrity reports1 invocation/1 journal/4 entries/1 terminal. First worker durable
handoff enqueue count is0: this case exercises NAK/redelivery recovery.

Independent review verifies actual SDK executable/build fields/clean Git source,
661 selected Git Go/module inputs before/after and3287 selected external
Go/Cgo/test inputs. That inventory is not exhaustive race/assembly/embed/compiler
provenance. Three native NATS servers are library instances embedded in the
captured SDK, with separate file stores; there are no separate server executable
processes. Fixture NoLog means server logs are unavailable.

All **4350 archive members** and **two parts** read back/hash verified.
Archive SHA256:
`0ca5070388ebb4858c23d07c06cd8a833b6a5d3423ba89683e67fd48423db359`.
Full proof includes source, selected external inputs, SDK bytes, original native
log, proxy transcript, raw journal/proof JSON and stopped stores.

This qualifies the SDK step-effect cancellation/late-result case under a real
uncertain lease-renewal response. Raw outer handlers blocking outside SDK effects,
worker process SIGKILL, partition combinations, independent stopped-store reopen,
physical replica drain, full matrices and24h are not covered. Queue drain and
all-peer checks have captured named-test assertion scope. The long soak and
million-timer candidate shared the VM; no pressure or historical server cause is
inferred. No unchanged simulation decision graph was rerun.
