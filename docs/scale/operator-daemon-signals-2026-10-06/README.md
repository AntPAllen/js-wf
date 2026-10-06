# Operator daemon signal lifecycle

Actual subprocess tests reproduce a CLI startup defect: SIGTERM during the first project metadata request returnedcontext canceled/exit1 in both default and actualR3 WFOPS domains. Registered shutdown cancellation now normalizes to success during projection construction and run. Independent errors remain errors; default/domain missing invocation-stream startups must exit1.

New signal cases cover project and tombstone-loop, startup SIGTERM and steady SIGINT, in both contexts. A controlled500ms initial client-trace hold reproduced the defect; retained corrected cases use2s to allow actual child executable/birth/argv capture before sending the signal. This is a test-boundary delay with real OS signals and real NATS peers, not a natural server/network fault. Steady projection readiness requires an actual waiting durable pull; steady tombstone readiness requires actual expired-state deletion. All eight child lifetimes must join cleanly, and two unrelated missing-source failures must remain nonzero. Original60s per context/3m SDK limits remain.

Corrected uncommitted race pair passes25.106s. Retained clean-source qualification is pending. Persistent command CI now includes daemon signals and preserves complete stores/source/process evidence. This does not substitute for the accepted full50000 SQL/domain case or qualify SQL daemon lifecycle, leaf routing, full fault matrices, million physical drain or actual24h.

## First clean native failure preserved

Clean f9706bd default daemon cases and domain project startup pass. Domain steady projection readiness (waiting pull) exceeds original60s; remaining domain cases and fatal control inherit the expired context. Actual child log contains only first metadata marker and no fatal error. Source1920 inputs, actual race SDK and six partial child executable/closure records, two closed store topologies and complete2380-member35,249,668-byte archive independently verify. No combined daemon qualification or server cause is claimed. [Original failure](initial-native-failure/).

The fixture now logs each administrative request and readiness errors and immediately reports child exit during readiness. Original60s/3m targets and waiting-pull criterion remain; corrected instrumented qualification is pending.
