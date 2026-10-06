# JetStream domain retirement across all-server SIGKILL

Prepared native fixture: `TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndAllServerSIGKILL`.

Three real server processes use domain `WFRETIRE` and original file stores. Each client is pinned to its server; both AccountInfo and the actual connected domain are checked. The existing strict retirement/reuse scenario drops a fresh-manifest publication reply, then reaps all three servers with SIGKILL before starting replacements. Replacement PIDs/server IDs and domain metadata recovery must be confirmed under the original 30-second whole-cut deadline. The original startup budget, 60-second scenario and production lease settings are retained.

This extends the native process fixture only. Native qualification is pending. Lease expiry, other domain cuts, legacy servers, active-writer GC, full matrices and actual24h remain separate requirements.
