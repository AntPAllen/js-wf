# Domain all-server retirement evidence

[Qualified original-target race case](qualified/): clean7a6531f, namedPASS31.78s,
all-three library cut/restore and domain API recovery5.810s, strict original
retirement invariants. Complete original SDK/source/stores retained/read back.

Earlier failures remain independent evidence:

- [Opaque callback failure](failed-post-heal-observation/):30.74s, strict completed-fault counter0.
- [Single5s post-heal request](failed-single-heal-request/):29.94s, three stopped/restarted IDs, zero heal responses.
- [Bounded retries under5s](failed-retried-heal-observation/):44.38s, same rejected gate.
- [Server/client boundary state](failed-five-second-meta-gate/):31.62s, running/domain configured/clientsCONNECTED, no local metadata leader at5s.

The original30s recovery deadline now covers the entire cut, not an added5s
metadata-election target. Earlier failures are not promoted; no NATS server
cause is assigned. Initial import compilation rejection and producer rejection
of uncommitted proof copies are retained. Full release requirements remain open.
