# Closed diagnostic copy recovery

At pushed f059bc8, the complete canonical base plus delta and all current captured
files were verified before removing only the closed diagnostic `copied-stores`.
The exact copy census, process/container closure, mount and visible descriptor
checks are in [recovery.json](recovery.json); descriptor visibility limits are
recorded. The executed procedure is preserved alongside it.

Recovered 1,984,749,568 allocated bytes; 2,427,334,656 bytes free at completion.
All remaining captured source, executable and metadata files are unchanged.
Original donors, canonical Git proof, caches and live stores remain retained.
The S3 delta alone requires its pinned canonical base for reconstruction.
No NATS process was started and no historical verdict changed.
