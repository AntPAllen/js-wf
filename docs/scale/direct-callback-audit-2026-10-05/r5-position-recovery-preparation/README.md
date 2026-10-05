# Explicit direct R1 consumer position-loss recovery

Observed same-owner restart preserved at7d79d13: assignment name/created/config/
owner survives, but actual API delivery and AckNone floor regress1961→1126 while
reader already accepted1960 source entries. Explicit direct callback reader now
captures its initial cursor identity/config and last successfully visited sequence.
On overlap it requires a fresh actual Info with identical name/stream/creation/
leader/complete config, R1 memory/AckNone, no outstanding acks/redeliveries and
matching positive delivery/floor positions strictly behind accepted source sequence.
Only then does common bounded recovery create a new cursor at the unvisited
sequence. Ordinary/unconfirmed overlap, changed config/identity/owner, equal/
ahead/zero positions and API semantic errors remain fatal. Public/default and
other readers are unchanged. Original20s/two resumes/leader gap oracle remain.

Targeted18case proof controls plus existing scanner/replay/cleanup/callback race
controls pass1.207s. Fresh actualR5 two-fixture native qualification pending, with
cursor snapshots/deletion replies/cleanup observations and closed proof preserved.
No large-fault/live/legacy/default/final matrices/24h acceptance yet.
