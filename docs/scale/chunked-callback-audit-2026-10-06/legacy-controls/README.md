# Chunked R1 legacy oracle comparisons accepted

Clean578c216 race test passes74.53s against three actual NATS2.11.17 processes.
All18 actual R1 cursor observations are chunked=true, MemoryStorage/AckNone/R1
against R3 sources. Full/captured cohort, compaction, fresh terminal state,
tombstone, corrupt snapshot and orphan reports/errors match independent point
oracle. No corruption tolerance or blanket replay was added.

Independent review verifies actual SDK/source/module/race metadata,686 selected
Go/module inputs, three actual legacy executable hashes and closed PIDs. Complete
1,070-member archive is48,939,101 bytes and SHA256
`0f53bf0cd64bc0552885ceb4ff836ed3e934ec02167a12a3063ea209f38eb9d0`.
All parts and member bytes independently read back. No process fault/full400k
capacity/live/default/full matrix/24h acceptance follows from this legacy control.
Next is the prepared fresh full400k/4.8M cold CPU and owner-down gate.
