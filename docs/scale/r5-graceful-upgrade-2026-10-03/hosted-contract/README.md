# Hosted graceful five-peer upgrade contract

Run37121460252 atcd59b86556cc04bc9524119dee6a7720a78ed32c is independently
accepted: actual positive race97.329s, single-old-peer control3.578s failure,
SIGKILL-substitution control6.085s failure at the required semantic assertions.
All five peers start2.11.17 and become2.15.0; six complete physical rounds
retain32 through37 matching message counts and last sequences on all5 replicas.

Every591 source hash matches Git before and after execution. Original-peer
Lame Duck notifications, ordered shutdown logs, exact-name exit/removal and
five distinct server identities are checked. Observed shutdown/removal spans
10.387–12.088s with30s configured eviction duration and10s grace.

Every680 original archive member SHA256 is verified locally. The unchanged
hosted archive is retained in two lossless byte chunks, with individual hashes
and verified concatenation SHA256. Concatenate parts in numeric order before
extracting. Terminal metadata, full original stores/binaries, commands, raw
events, controls and independent review are retained.

The first selective reviewer extraction omitted the config suffix; the original
verified archive already contained it. Including that config completed the
independent review without changing any hosted evidence.

This establishes the constructor/retained-data graceful upgrade contract.
Sustained mixed workload, forced Start gap,200 seeds and24h are separate gates.
