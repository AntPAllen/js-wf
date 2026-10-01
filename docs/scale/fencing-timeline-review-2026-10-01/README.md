# Reproducible fencing timelines

Run `python3 scripts/review-tier3-fencing.py --root ARTIFACT_ROOT --output FILE`.
Plain JSON and retained gzip files are supported. The reviewer joins each fencing
record to exactly one original delivery fetch and one invocation journal terminal
timestamp. This separates already-terminal duplicate fetches from completion
during a delivery, while an owner is stopped, or after fencing. It retains fault
intersections, original delivery steps and later invocation/exact-delivery ack
observations. These local ack observations alone cannot prove broker commit.
No server root cause, independent retained-store audit or full-release claim is
inferred. Existing history, invariant, p99 and physical drain gates remain needed.

All50 Python tests pass. Controls reject missing/wrong/duplicate fetch records,
missing terminals, reversed times and classify nanosecond boundaries correctly.
Frozen journal, consumer, all-server, fan-out, quorum route, majority route and
pause evidence all reverify. Derived summaries/full compressed reviews live beside
the original proofs. One restart description is corrected: its terminal preceded
fencing but followed fetch, so this was completion during the original delivery.
Original artifacts and acceptance are preserved.
