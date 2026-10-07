# Combined leaf recovery proof

Native race test passed at source `a31994f`: actual leaf SIGKILL, production 12-second lease expiry, graceful restart of all three embedded hubs, and controlled weak-frame absence through the remote domain. The strict retirement/reuse scenario passed unchanged. Whole recovery cut: 21.33 seconds.

Independent review verified the actual SDK, both stock leaf process identities, captured source, complete archive and 42 rejected proof mutations. Exact weak object identity is bound to the captured native confirmation log.

This qualifies one recorded scenario. It does not establish natural follower lag, all-hub SIGKILL, daemon-child transport, or the full fault matrix. Archive metadata and inventory are retained here for S3 offload and safe local cleanup.
