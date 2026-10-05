# Callback fullaudit consumer fault race controls

At e792fa8 native race TestStreamingAuditNativeJournalFaults/callback-delivery
passes38.70s. Consumer leader node1 is shut down through the server library with
4039 records pending on the replicated memory/AckNone consumer (not OS SIGKILL).
Full1500 invocations/journals/terminals and6000 entries pass in1.938723225s under
the unchanged20s budget. Large16KiB journal payloads cross production windows.

Cancellation at journal record128 returns context.Canceled and only the partial
1500-invocation report in340.466680ms. Native checks both audit streams have zero
consumers after each case.

Independent selectedsource/actualSDK/race/module/processclosure review and
complete closed fixture/archive readback succeed. Exact archive part/member/byte
counts and SHA are in archive-verification.json. Defaults unchanged; legacy/full
400k/live24h acceptance still pending.
