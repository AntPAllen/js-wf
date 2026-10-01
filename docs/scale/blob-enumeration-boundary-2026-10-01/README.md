# Real blob and state enumeration boundary

The opt-in TestQuiescentBlobSweepPaginationBoundary creates 100,005 real
unmanaged one-chunk objects and 100,005 KV state keys on the normal R3 cluster,
plus a referenced runtime result, an orphan and a deleted-object tombstone.
The final lexicographically ordered state key contains the sole protected
result reference, beyond the server's 100,000-subject page. Independent raw
stream censuses require the exact KV subject count and more than twice the
object population in Object Store subjects. Writers stop before sweeping.

Two actual production sweeps enumerate count+2 live objects and one reference:
a 24-hour age guard deletes zero, then zero age deletes exactly the orphan.
The protected result and bracketing unmanaged payloads remain byte-identical;
the orphan and previously deleted object must be absent. This exercises both
metadata pagination paths and physical Object Store data, not synthetic
metadata. It does not establish online GC or the earlier intermittent listing
mismatch's cause.

The 100-object diagnostic race passes in 4.505 seconds. A compiled production
StateKeys overlay omits the final sorted key; the same diagnostic fails in
3.158 seconds on Referenced=0 versus 1, before the destructive pass. This is a
semantic failure, not a timeout/build error. Vet passes. The diagnostic is
explicitly below the pagination boundary and is not full scale evidence.

The registered journal-boundary-100000 workflow now has scope selectors for
all/journal/continuation/blob, defaulting to all. Its blob job always uses the
full 100,005 count, retains the log and bounds the Go test to 25 minutes.
Full hosted pagination confirmation is pending.

    WF_BLOB_ENUMERATION_SCALE=1 go test ./integration -run '^TestQuiescentBlobSweepPaginationBoundary$' -count=1 -timeout=25m -v

WF_BLOB_ENUMERATION_COUNT may lower local diagnostics to 100..100005;
only count greater than 100000 counts as the combined pagination proof.
