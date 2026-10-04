# Verified duplicate raw expansion recovery

Published proof commit `426e8102fc1f4aeffb1d5572a8e5da889c32607e` was verified at remote main.
All committed proof parts and concatenation, canonical archive members, original
ZIP members and every expanded raw input independently hash-verified before
removal. Every target had no visible open file descriptor. Only the200 duplicate
`seed-N/raw` directories were removed, recovering3,851,362,304 exclusive
allocated bytes. Original ZIPs, canonical archive, source/model/reviewer files,
metadata and Git proof parts remain. Failed evidence and live soak stores are untouched.

Restore raw files from the canonical archive into a fresh directory, or validate
and re-extract each retained ZIP before replaying a row review. The recovery
changes storage only; all qualification scope remains as previously documented.
The exact executed script and per-seed hash/descriptor/recovery record are retained.
