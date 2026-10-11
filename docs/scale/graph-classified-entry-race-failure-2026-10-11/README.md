# Classified 100,000-entry race failure — 2026-10-11

The frozen source 9189259faca292659001bbbcbf936c73a1e00318 race campaign
closed with child and supervisor exit 1 after 14,645.931 seconds. It did not reach
100,000 entries or a completed checkpoint. The observed journal count and tail were
40,959; an append at index 40,959 returned an uncertain outcome with a request
deadline. This does not establish whether that append committed.

The single emitted profile window contains 472 definite CAS conflicts and one
ReadBlob deadline. Its requested window end (24,996) is not a completion count.
The package watchdog had not expired. A diagnostic root read also timed out.
Server-side cause remains unconfirmed. This source predates the SDK admission fix;
the failed run provides no evidence that the new fix caused this failure.

The normal 100,000-entry proof remains accepted separately. Race acceptance was
explicitly refused with actual exit 1. The independent failure reviewer verifies
1946 unchanged source inputs, the actual race binary, terminal unit and process
receipts, exact logs, and all 866 retained native files (685,671,380 bytes).
`review.json` verifies failure evidence while keeping acceptance false.

Native storage, the executable and frozen checkout remain local for recovery
investigation. Re-run `python3 docs/scale/graph-classified-entry-race-failure-2026-10-11/review.py`
from the repository to check the retained census. No replacement campaign has
been launched. Recovery classification and full 100,000-entry race qualification
remain open.
