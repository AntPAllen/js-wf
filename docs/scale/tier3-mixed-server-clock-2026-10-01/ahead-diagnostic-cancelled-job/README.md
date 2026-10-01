# Ahead diagnostic job state mismatch

The selected test step in job110627389113/run36939480705 was reported as
cancelled at2026-10-01T23:15:45Z during cold fixture startup. Its downloadable
log includes cancellation and cleanup, and uploaded Go events contain only
package start, named test start and RUN output. No test verdict, backlog
snapshot, clock admission or final audit exists. The log states only that
the operation was canceled; initiator/cause is unknown.

Both the run API and latest direct job API still report in_progress with no
conclusion. The job is therefore not treated as terminal. The contradictory
step/log observations are retained; this is not acceptance or evidence of a
runtime failure. Continue observing the same handle; no replacement launched.
