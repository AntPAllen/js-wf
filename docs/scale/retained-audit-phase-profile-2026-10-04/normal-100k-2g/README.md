# Full100k native audit with explicit2GiB: complete within original20s

Actual source `4b7531e552c71ef7d2971b80073c16f7dd97950f` runs the same native
100000-invocation /1200000-entry fixture with GOMEMLIMIT=2GiB, GOMAXPROCS=2,
without race. Actual SDK SHA256 is byte-identical to the failed512MiB run:
`84564d8c5f8746174e4b96fb0f5e647974ccb4e3763d554511d0b96289325dbb`.
The source revisions differ only in evidence/documentation, not the SDK inputs.
Independent review verifies the exact2GiB environment, binary, source and live
/proc identity. Both fresh clusters have the same logical handcrafted population;
these are sequential single samples, not paired physical stores or distributions.

| Phase | Wall time |
| --- | ---: |
| WF_INV scan including visitor | 1.526244348 s |
| WF_JRN scan including visitor | 9.095523142 s |
| Journal visitor decoding/grouping | 2.167972438 s |
| Full audit | 15.487657247 s |

All100000 journals,1.2M entries and100000 terminal values qualify the same
full checker under unchanged20s. Source streams remain three replicas; audit
consumers match stream replication and batches remain512. Profile includes
client and all three embedded servers in one process. GC scan cost is lower
than512MiB's profile, consistent with memory sensitivity; this does not isolate
five-container worker/checker costs or prove a NATS cause. A normal-build memory
comparison does not qualify fault capacity, a race campaign, full matrices or24h.
The failed512MiB result remains preserved and failed.

All2889 selected inputs/59 Git files, actual SDK and every build-info field,
exact named-test/record counts and profile verify. Complete proof preserves
3326 original members /600686934 bytes, source copies, actual SDK, stores,
events, derived pprof commands/output, and review scripts. Every original/member,
all five parts and concatenation SHA256 verify by readback. Compressed proof:
111206526 bytes, SHA256
`6cb26f762073adfc366a33bf368e76a1ed5bccf18e389a53c28c10b73301a2ad`.
Stores are not independently reopened; selected sources exclude exhaustive
assembly/embed/generated/hermetic proof. Test process is terminal.

The five-container runner now exposes --memory-limit with an explicit bounded
choice and records it in execution/test-environment evidence. Default512MiB
and race remain unchanged. Next bounded real five-container journal diagnostic:
--duration10m --no-race --memory-limit2GiB --retained-audit-trace
--batched-retained-audit. No new24h restart follows from these results alone;
large-population fault recovery and complete release profile still require proof.
