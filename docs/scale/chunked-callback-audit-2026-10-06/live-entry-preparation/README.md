# Explicit live entry points after full capacity qualification

Public explicit CheckWithChunkedConcurrentStateReads and its captured-cohort form
use the same bounded chunk scanner/state snapshot/reduction and R1 memory cursor
configuration. Creation validates actual cached identity/MemoryStorage/AckNone/R1;
retained source replication and leader gap/absence oracle remain unchanged.
Default Check and existing reader profiles remain unchanged.

Matrix checkpoints/final audits select the same new explicit mode. Producer flag
--chunked-state-retained-audit requires --memory-limit4GiB and selects4CPU/GOGC500,
matching recorded fullsize cold/owner-left-down/samestore restart qualification.
Allfour reader modes are mutually exclusive. Original20s/60s/threeattempts and
p99/heal/physicaldrain/cardinality gates preserved. Ten Python profile controls and
Go race retry/conflict/callback/position controls pass (integrity1.022s/integration
1.029s). Current2.15.0 library race oracle comparisons through the new entry scanner
are prepared; sustained live/default/currentmatrix/actual24h remains unqualified.
