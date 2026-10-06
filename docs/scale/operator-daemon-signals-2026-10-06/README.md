# Operator daemon signal lifecycle

Actual subprocess tests reproduce a CLI startup defect: SIGTERM during the first project metadata request returnedcontext canceled/exit1 in both default and actualR3 WFOPS domains. Registered shutdown cancellation now normalizes to success during projection construction and run. Independent errors remain errors; default/domain missing invocation-stream startups must exit1.

New signal cases cover project and tombstone-loop, startup SIGTERM and steady SIGINT, in both contexts. A controlled500ms initial client-trace hold reproduced the defect; retained corrected cases use2s to allow actual child executable/birth/argv capture before sending the signal. This is a test-boundary delay with real OS signals and real NATS peers, not a natural server/network fault. Steady projection readiness requires an actual waiting durable pull; steady tombstone readiness requires actual expired-state deletion. All eight child lifetimes must join cleanly, and two unrelated missing-source failures must remain nonzero. Original60s per context/3m SDK limits remain.

Corrected uncommitted race pair passes25.106s. Retained clean-source qualification is pending. Persistent command CI now includes daemon signals and preserves complete stores/source/process evidence. This does not substitute for the accepted full50000 SQL/domain case or qualify SQL daemon lifecycle, leaf routing, full fault matrices, million physical drain or actual24h.

## First clean native failure preserved

Clean f9706bd default daemon cases and domain project startup pass. Domain steady projection readiness (waiting pull) exceeds original60s; remaining domain cases and fatal control inherit the expired context. Actual child log contains only first metadata marker and no fatal error. Source1920 inputs, actual race SDK and six partial child executable/closure records, two closed store topologies and complete2380-member35,249,668-byte archive independently verify. No combined daemon qualification or server cause is claimed. [Original failure](initial-native-failure/).

The fixture now logs each administrative request and readiness errors and immediately reports child exit during readiness. Original60s/3m targets and waiting-pull criterion remain; corrected instrumented qualification is pending.

## Instrumented clean native case accepted

Clean `4a1f1bf895b32e65f238c324d87d1f4fdfc389d2` passes25.2741s actual race SDK. All eight real signal children join with exit0: default/WFOPS × project/tombstone-loop × startupSIGTERM/steadySIGINT. Two actual missing-source controls exit1. Three actual domain peer admissions verify; steady readiness remains waiting durable pull and deleted expired tombstone. Per-context60s/SDK3m and original signal criteria remain unchanged. Readiness polling changes10ms→50ms and logs request progress/errors; the prior clean failure's cause remains unconfirmed.

Independent review binds1921 exact Git/before/after/current retained inputs, actual SDKrace/count1/3m/GOMAX2/1GiB/packagecwd/hash/closedPID, all eight recorded child executables/birth/actual helper argv/selected operator arguments/closure and exact logged exit identities, two closed1/3-node store topologies and all2355 archive members. Complete35,244,544-byte archive SHA73c61d206cfbc03c2fd35a1be04fe73047b0e73fde5e61fd80a193effd0279e2 verifies against original files. Two new daemon guard groups reject nine coverage substitutions and original native failure; the earlier command guards remain passing. [Complete review](native-race/independent-review.json).

This qualifies the CLI core signal boundary in actual SDK subprocesses with controlled startup trace delay and real OS signals/NATS peers. It does not claim a standalone deployed wf binary, natural network/server faults, SQL daemon lifecycle, leaf routing, fullmatrix, originalmillion drain or actual24h acceptance. Original failed proof remains preserved in S3; no prior server-cause or pressure attribution is made.

Complete accepted archive, metadata and inventory have verified full S3 body readbacks; see [receipt](native-race/s3-readback.json). Initial failed native evidence remains separately preserved. Original fixtures stay closed and retained.
