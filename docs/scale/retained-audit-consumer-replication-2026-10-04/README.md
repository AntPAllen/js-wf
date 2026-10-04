# Audit consumer replication: no clear speedup observed

Actual retained race SDK at `ddd5396065c3d1fbe41ef7720d6b14948283059d` audits the same quiescent handcrafted
12,000-invocation /144,000-entry /12,000-terminal stores three times:

| Temporary consumer | Elapsed | Result |
| --- | ---: | --- |
| Three replicas, before | 12.024088088 s | Complete |
| One replica | 12.653120485 s | Complete |
| Three replicas, after | 14.682479197 s | Complete |

All three reports match. Source streams retain three replicas throughout.
Actual single-replica consumer configs are checked for both WF_INV and WF_JRN;
final consumer counts are zero. This does not demonstrate a clear replication
speedup. Production continues to request stream-matched consumer replicas and512
pull windows. Candidate fault recovery is not qualified; no soak is restarted.
The native source's delivery-update replication suggests a hypothesis, not a
confirmed cause of the original runtime audit failure.

Independent review verifies all2889 captured inputs /59 local Git bytes, actual
runner, live SDK executable digest and every build-info field. Actual SDK:
`a928981c462d457dc9c18358775c120e2af19ab00fa56326007ff1cff63450cf`. [Complete proof](complete-proof/) preserves
3270 members /158236551 bytes, all original stores, executable,
selected sources, events and exact producer/reviewer. Compressed archive:
43533179 bytes in two parts, SHA256 `072511249f8b1a83655d7922dcdc0dbc2d97028e1d9544d6a7770352c0833c36`.
All input/member and part/combined archive hashes verify by readback. Stores
were not reopened. Concatenate numbered parts, verify hashes, extract into a
fresh directory. Final-source matrices, original24h attempts and million-timer
physical drain remain open; this comparison does not qualify them.

Next diagnostic adds a CPU profile and separate stream-scan/visitor timings
under the same20-second whole-audit limit. The in-process native profile includes
both client and embedded server activity; it is not a real five-container soak.
