# Delivery timestamp representation correction

NATS client1.54 delivery metadata and the retained compact parser construct
`time.Unix(0,nanoseconds)` timestamps; point queries decode the server's RFC3339
UTC representation. Equal instants can have different Go locations and fail
reflect.DeepEqual. A dedicated equal-instant/distinct-location control establishes
this representation discrepancy. Invocation, journal and signal bulk timestamps
now normalize to UTC, preserving every instant and delay. Three projection/time
control groups pass under race in1.017s.

The first failed native comparison is fully preserved. It did not record individual
mismatches, so that archive alone does not establish its exact first mismatch or
exclude additional problems. The new native fixture writes ordered comparison
status and first mismatching point/bulk samples with Go time representations before
failure. Fresh full160-workflow correction qualification is pending. No original
or failed store is reopened; no server defect or large-cohort adoption is claimed.
