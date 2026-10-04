# Executed-source Tier2 matrix progress

Campaign [37149506857](https://github.com/AntPAllen/js-wf/actions/runs/37149506857)
executes exact `c4fed061bc614488d4f89b53b216b756490f7da0`.
Four successful journal-row shards are independently reviewed:

| Seeds | Invocations | Journal entries | Leader faults |
| --- | ---: | ---: | ---: |
| [1–12](journal-1-12/) | 31,472 | 346,713 | 228 |
| [13–24](journal-13-24/) | 32,032 | 352,970 | 228 |
| [25–36](journal-25-36/) | 31,864 | 351,103 | 228 |
| [37–48](journal-37-48/) | 31,388 | 345,812 | 228 |
| Total | 126,756 | 1,396,598 | 912 |

Every seed executes the full ten-minute workload. Raw named-test/package
events, latency samples, fault chronology and all three history models verify.
Worst per-workflow terminal/progress p99 are 17.457496447/7.305191192 seconds.
All 394 archive members reopen and SHA-verify. The source-bound original uploads
do not contain actual binaries, compiled-source ledgers or physical stores;
their provenance and audit scope are explicit in each shard README.

These results accept journal seeds 1–48 only. Both full-release flags in
`summary.json` remain false. The campaign continues with later journal seeds;
all 200 seeds of each of the 13 rows and actual 24h/five-VM-or-container requirements
remain open. The prior older-source shard and cancelled campaigns retain their
earlier scopes and cannot substitute for this campaign.

All four accepted raw expansions are later removed only after all 394 canonical
members, 288 expanded raw files and committed Git archive blobs reverify with no
open descriptors. Complete compressed originals remain locally and in Git;
restore before offline review. [Exact recovery proof](../tier2-first48-duplicate-recovery-2026-10-04/)
records 2,712,244,224 allocated bytes recovered without changing qualification.
