# Preserve expired pull context classification — 2026-10-11

The pinned NATS SDK v1.54.0 FetchContext returns ErrInvalidOption with the text
"context deadline already exceeded" when its deadline has already elapsed. It
does not wrap context.DeadlineExceeded. In the worker adapter, descheduling
between creating the one-second pull context and SDK option processing can
therefore produce a fatal configuration error. RunPartitions propagates a
partition's fatal error to the entire assigned group.

The adapter now joins the attempt's actual context cause with a synchronous
Fetch error before canceling the attempt. Both errors remain discoverable.
An expired pull remains a bounded empty-poll outcome in the existing dispatch
loop. Healthy-context invalid options remain fatal. No timeout, retry policy,
SDK configuration, or unknown append policy changes.

The new control delays the first real SDK Fetch option evaluation by 1.1 seconds
and runs the production dispatch adapter and loop over a native consumer. The
baseline exits before dispatch with the exact opaque SDK error (handled=0,
calls=1). Its go-test output is retained; no baseline executable hash was
captured. The corrected control continues to the second Fetch, delivers the
original wake payload, confirms its ACK, and stops through parent cancellation.
A separate healthy-context invalid-option cut verifies one fatal call and no
handler execution. Existing informational-reply, cancellation-before-transport,
native delivery and stalled-batch cancellation controls are included.

`qualify.py` records all Go/module input bytes and builds two retained actual
executables, normal and race. Each exits 0 with five top-level named tests and
both new subcases passing. `review.py` checks executable hashes captured through
/proc, terminal exits, named controls and unchanged inputs. The main CI already
runs all worker tests normally; a targeted race step now runs these controls.
No hosted CI completion is claimed.

Scope: reproduced local pull-classification defect and bounded dispatch controls.
The seeded 163-family/946-pin corpus was not rerun or changed. This does not
accept the prior native active-population failure, prove its cause, or qualify
the full implementation plan.
