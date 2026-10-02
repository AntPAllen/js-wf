# Admitted behind ten-minute attempt: third-cut admission failed

Run36965338232 at1eda169 is terminal/failed after169.592s package time. It
completed two admitted skewed-leader removals and a batch10 retained checkpoint
of280 invocations,280 journals,3,084 entries and280 terminals. The third
admission found no provable pending shifted-native timer within its10s budget
and canceled the fixture. There is no final completion, drain or sustained-row
acceptance. Original uploaded artifacts, failed-step log and terminal API
snapshot are compressed with original SHA-256 hashes.

Batch11 has three first-duration2s requests with canonical utc-quorum-v1 origins
at04:42:49.738–49.742 and Suspended receipts at49.792–49.798. Their native
schedule acknowledgments at49.778–49.783 contain healthy server timestamps
near49.762–49.776, not the required minus60-second source. They therefore cannot
qualify for a cut proving a timer created on the shifted source. The admission
correctly rejects these requests; this is not a green third cut. The origin
validity and initial healthy native-clock observations are directly in retained
operations/receipts. Why these waits do not progress into a later qualifying
request before cancellation remains unconfirmed. Do not widen or remove the
source/cut checks merely to produce a passing row.
