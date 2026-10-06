# Restored pre-audit consumer count confirmed

Clean058040c fresh verified full400k/4.8M R5 copied fixture fails16.30s during
initial diagnostic inventory, before creating any current audit cursor or starting
the20s baseline. WF_INV reports0 consumers and a successful empty list. WF_JRN
already reports2 consumers; ListConsumers cannot obtain infos within its1s probe
and returns context deadline exceeded. This confirms a pre-existing count, not
consumer identities, actual live assignments, or an isolated NATS cause. No
journal audit, CPU profile, or owner SIGKILL occurs in this fixture.

Earlieracc9123 completed exact full read/reduction17.114920s and failed the zero
count gate; its failure remains. This preflight evidence identifies a fixture
precondition that must be investigated before repeating bulk capacity tests.
Next instrumentation asks ConsumerNames before consumer Info to distinguish
assignment names from missing Info responses, retaining the same bounded probe.
No restored consumer was deleted; the zero-count assertion remains.

Initial reviewer incorrectly required a successful inventory even on failed native
preflight. Its script and terminal error are retained; corrected reviewer handles
preflight failure without inventing audit results. Independent source/actualSDK/
five server bytes/modules/mounts/closedPIDs/1058 unchanged donor files verified.
Complete1714-file830alias lossless reconstruction; delta50,117,678bytes SHA256
`c41e474908a6c19e1b6088fb95c6b54ba98ccf01a7c1ae979e921fb91e2b64a0`.
Both pinned canonical donor Git parts and delta parts required; fixtures stay
closed. No capacity/fault/default/live/fullmatrix/24h qualification.
