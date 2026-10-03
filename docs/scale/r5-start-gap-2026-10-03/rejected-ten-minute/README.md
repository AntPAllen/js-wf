# Rejected ten-minute forced Start-gap profiles

SIGKILL run37128612951 and graceful Lame Duck run37128612942 both execute exact
9ac3ad44267b68d1a1ec63ce47e1fb11d9c00204. They fail after their third forced gap,
sequence1,270, before reaching full duration or final audit/drain. First two
gaps complete within30s; the third has no recorded repair event. Raw invocation
identity is retained unchanged across all three upgrades. Neither is a qualified
release row.

The complete unaltered uploaded archives are split into40MiB parts. Each part,
concatenated original archive and every original member are verified. Terminal
metadata and failed job logs are retained alongside independent reports.

Reproduce from the repository root:

```sh
python3 docs/scale/r5-start-gap-2026-10-03/rejected-ten-minute/review.py docs/scale/r5-start-gap-2026-10-03/rejected-ten-minute/sigkill
python3 docs/scale/r5-start-gap-2026-10-03/rejected-ten-minute/review.py docs/scale/r5-start-gap-2026-10-03/rejected-ten-minute/ldm
```

Originals lack Start scan progress, so exact cursor/server cause is unconfirmed.
The production scanner's32/1s capacity needs39s to reach1,270 from cursor1 with
instant reads. A deterministic counterexample and faster-policy control are
retained separately; new real runs must supply actual scan progress.
