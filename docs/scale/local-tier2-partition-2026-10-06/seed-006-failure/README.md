# Local partition campaign seed6 failure

The original fixed200-seed coordinator stopped on its first failure at20:10:01UTC. Seeds1–5 exited0; only seed1 has independent raw qualification so far. Seed6 exited1 after70.41s with a replica-recovery deadline failure: `KV_WF_LEASE` replica `wf-process-2` was non-current with reported lag3535. Seven mixed batches completed before failure. Broker log tails show repeated Raft catchup warnings; these do not establish the cause or prove a NATS defect.

Original10m/count1/nonrace source300a36a and18m SDK selector were unchanged. No retry, timeout relaxation, campaign restart or original media inspection occurred. Original24h journal campaign remains separate. This failure invalidates full200-row qualification.

Complete closed seed root is preserved, with hardlinked regular inputs dereferenced into the archive. The campaign checkpoint and exact source/command/execution metadata are committed beside the full inventory. Shared campaign preparation/cache paths remain retained locally; any future restored independent audit must explicitly account for the recorded original paths. Historical brokers must be opened only on fresh verified restores.

Archive members=6166; bytes=74726407; SHA256 `37dced6bdb8afd9d0718064d77f31a9437d650c0dd408bdf68cb450303691db1`. Full remote S3 custody is recorded separately in `s3-readback.json`.
