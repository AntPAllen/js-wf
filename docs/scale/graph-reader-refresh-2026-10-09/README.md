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

## Frozen native materialized-reference fixture accepted

Both selected fixtures pass from the clean frozen **9cbdfbc** repository:

| Fixture | Actual test duration | Remaining two-minute fixture budget at final assertions |
| --- | ---: | ---: |
| R3 domain | 95.35 s | 25.358 s |
| R1 | 64.23 s | 55.873 s |

Both actual child exits are zero. They execute the same retained race binary,
SHA-256 `33090cf0a5feeea056699ad7bba271ebea2623a2b0ce2fd3df8916c94405bd73`.
The compile exit is zero and its build metadata confirms `-race=true`.
Precompile and post-execution inventories match all 1,819 retained repository
Go/module/trace inputs; the executed review also compares those hashes to
the frozen Git objects and verifies the checkout and binary remain unchanged.
The sparse checkout excludes repository documentation to avoid duplicating
32 GiB of unrelated artifacts; all selected test/compiler inputs are retained.
This records repository-source provenance, not an independent attestation of
every external module/toolchain byte.

The two-minute fixture deadline, all 131 prefix records, ownership/malformed
frame controls, missing/corrupt metadata controls, old-source negative read,
three materialized payload edges, bounded anchored restore, two leased worker
executions, exactly one stage call and result 42 remain verified. Both logs end
with `blocked_prefix_reads=0` and actual PASS. Public constructor admission and
production collection remain disabled; this is not native kill/limit, complete
current-suite, matrix, clock/storage, deployment or release qualification.

Prefix construction's SDK request count decreases from 15,443 in the earlier
range-only R3 diagnostic to 14,264 here, consistent with replacing the 131
acquire/release pairs. R3 checkpoint discovery/publication takes 4,366 requests,
versus 10,570 before ordered traversal. Wall times are observed runs under
concurrent campaigns, not a controlled isolated speed benchmark or a diagnosis
of the earlier timeout's entire cause. Previous failures remain retained.

[Executed independent review](executed-review.json), [review script](review.py),
[R3 commands/exits](native-command-results.json), [R1 command/exit](native-r1-command.json)
and raw logs provide the component evidence. Both frozen full151 race and
frozen full155 normal remain live on older source cuts; complete current
156-family/all-841-pin and every original broader requirement remain open.
