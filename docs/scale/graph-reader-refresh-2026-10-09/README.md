# Atomic reader refresh

`Protocol.ReplaceReader` replaces one exact live reader pin with a fresh ID
and the current canonical main/named forests in one original-head root CAS.
It does not consume an extra reader slot, accept a caller-supplied snapshot,
shorten expiry, or resurrect expired/released handles. The old handle and its
serialized checkpoint are revoked even when the captured forest is unchanged.
The collection clock is checked after the authority read and before CAS.

`GraphView.Refresh` validates destination, generation and active lifecycle,
and installs its new handle/cursor only after confirmed success. It retries
only definite conflicts with fresh generation observations. Lost acknowledgments
retain the existing exact original-head witness rule; a failed or later-head
witness cannot resolve uncertainty. Worker append/readback uses this refresh.
An expired/revoked handle retains the existing fresh-open recovery path.
Public continuation admission and production collection remain disabled.

Focused race controls pass with actual exit zero:

- graphpublication: 1.119 s, unchanged/named forests/full reader slots, old handle
  and checkpoint revocation, lost ack, expiry during authority read, shortened
  expiry, duplicate ID, release, unknown reads/replies, advanced-head witness
  rejection and definite conflict.
- journal: 44.788 s, all 16 archive/collection scenarios plus destination,
  invocation, retirement, unknown outcome and one-CAS controls.
- worker metadata envelope control: 1.073 s.
- final journal refresh controls: 1.186 s, including the corrected expiry cut
  installed after the independent census so expiry happens inside Refresh's
  observation, rather than before the call.

Commands, actual exits and source hashes are retained alongside the raw logs.
The frozen native R3-domain materialized-reference fixture is a separate check;
these controls do not qualify native recovery or the complete current suite.
