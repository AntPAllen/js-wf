# Explicit four-core full-population comparison

Prepared comparison, no result claimed. This uses the existing plain copied-store
gate with GOMAXPROCS=4, GOGC=200 and GOMEMLIMIT=2GiB. The five restored servers
retain R5 file storage, explicit routes and a two-minute sync interval.

The original 400,000 invocations, 4.8 million journal entries and 400,000 matching
terminal states must be present. SDK/concurrent, compact/concurrent and SDK/recheck
each retain the original 20-second deadline. The candidate must return the exact
full report within that deadline; a timeout remains failed.

The producer checks the canonical archive in Git, then verifies original and fresh
disposable-copy file hashes before opening the copies. Original stores stay closed.
It captures selected source, actual SDK/server bytes, environment, mounts and
process closure. The existing two-core failures remain preserved.

This measures a changed CPU/GC configuration while retaining the same Go memory
target. It cannot attribute any difference to CPU count alone, establish a default
reader adoption, or qualify a live 24-hour campaign.
