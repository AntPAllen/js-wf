# Original rolling-upgrade physical queue diagnostics

The failed123bc4b seed1 smoke residual run sequence494 has a successful confirmed ACK on delivery4 before its recorded upgrade healed. Its later native drain diagnostic still retains the message while64durables report zero pending/ACK pending. Prior fresh copied restart clears it. Historical cause remains unconfirmed; no original stores reopened.

Original Tier2 upgrade now retains bounded concurrently joined snapshots from all three pinned NATS monitoring ports before each upgrade, after confirmed healing, and after native drain success/failure. Raw JSON/errors/start+finish times preserved. No ACK/purge/restart/repair is performed by monitoring. Uses existing ProcessCluster.Diagnostic and public /jsz; snapshots are not simultaneous. Original workload/upgrade/recovery/latency/history/integrity/drain budgets stay unchanged.

Prepared normal ten-minute seed1 with captured2.11.17binary. Native reviewer checks original SDK/source/helper/profile/closure/allcells plus three exact version transitions/originalcadence and complete physical peer snapshots; requires every final physical run stream empty. Native terminal and copied-store qualification pending. Failed reviews retain full evidence; existing24h/million sources/handles unchanged.

[NATS monitoring documentation](https://docs.nats.io/learn/monitoring/monitoring-endpoints).
