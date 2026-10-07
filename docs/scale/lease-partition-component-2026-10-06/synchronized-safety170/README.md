# Full synchronized Raft comparison

Orchestrator c048bcc; actual compiled parent 9af9a65. Both upstream and contiguous candidate race binaries pass all **170 original TestNRG tests**, count1/20m, GOMAXPROCS2/GOMEMLIMIT2GiB. No skip, failure, or race report. Actual live executable bytes, birth, argv, cwd and environment, source closure, all selected inputs and complete fixture archive independently verified.

The setup overlay adds eight lock/unlock pairs around 17 peer additions in two tests and reserves all snapshot I/O permits in a third. No diagnostic late-return injection. Original assertions, subcases, sleeps and deadlines are unchanged. The durable user service exited 0 without restart.

This qualifies this complete derived comparison, not the earlier unmodified failed suites, production dependency adoption, workflow fault matrices, Tier1, million-timer drain or 24-hour soak. Historical snapshot-failure holders and interrupted-run termination cause remain unconfirmed. The default dependency is official NATS v2.15.0.

Full archive: 89,364,246 bytes / 5,338 members, SHA256 `3854bc4dcb0b0a64545355d000e561dfb60cabc3fd241e8c7b841f729601047c`. Source and execution ledgers, raw logs and independent review are retained here; the complete archive is retained in content-addressed S3 storage with a fresh complete byte readback; see [receipt](s3-readback.json).
