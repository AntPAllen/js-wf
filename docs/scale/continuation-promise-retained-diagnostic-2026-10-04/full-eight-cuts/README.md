# Full eight-cut continuation/promise native test accepted

Executed source `58fd143108f7a450b45ea8c3cfe22b7e231a6d00`. The parent and all eight named subtests ran once, passed, and did not skip. Package 142.322 s; wall 142.541030280 s. Each cut confirms actual worker SIGKILL, all three NATS process restarts on their original stores, replica readiness and successful successor completion through another pinned node. Native 90 s context, 25 s readiness and strict 30 s kill/signal recovery bounds remain unchanged.

| Cut | Test seconds | Kill recovery seconds | Signal recovery seconds |
| --- | ---: | ---: | ---: |
| `before_frame` | 18.13 | 12.732018607 | 5.109592732 |
| `after_frame` | 18.24 | 12.735891185 | 5.108559236 |
| `before_manifest` | 18.74 | 13.085523217 | 4.412019534 |
| `after_manifest` | 18.12 | 12.665077005 | 5.107764881 |
| `after_journal_purge` | 18.59 | 13.026062693 | 6.509591910 |
| `after_signal_purge` | 18.67 | 13.033255122 | 4.410515557 |
| `after_suspended` | 18.53 | 12.994329902 | 6.507997765 |
| `after_handoff_release` | 13.3 | 7.704091233 | 0.109845474 |

The source-bound named tests assert confirmed prefix preservation after restart and successor takeover, higher successor epoch, correct child result, no child effect replay, exact initial effect accounting for published/unpublished frames, correct child declaration/consumption, two terminal invocation integrity, identical result through all peers, offline continuation replay, child-result preservation during retirement and final deletion of three blobs with zero references. Published frames resume with one child blob read and zero archive reads. `after_frame` collects one abandoned candidate; other cuts collect zero abandoned candidates. All pre-kill journal/checkpoint read audits complete without primary errors or deadline flags.

This qualifies the complete eight-cut native test at its executed source. It did not reproduce or explain the original historical pre-kill read failure, repair that failed CI parent, qualify either full matrix, independently reopen physical stores, or qualify the actual 24-hour soak. Named-test assertions are source-bound provenance rather than a separate raw-state history audit.

Independent review verifies all 3,849 selected pre/post/captured source inputs, including 562 local inputs against executed Git. The inventory includes dependency test files; it is not exhaustive assembly/embed/generated or hermetic compiler provenance. Actual SDK executable/build info and all eight actual NATS executable copies/build info are retained. The SDK inventory does not bind every NATS server main input. SDK SHA: `8af456001b867e1a4cdbf30c5bdbf1cbc082990a5647e17cebda0c1d5cf947b7`; NATS SHA: `24759ea94c030a9c5f1cd7fc61c74e14615a3c35650b5bb28b29891b5899ba9d`.

All 3,416 stopped-state original files (217,906,674 bytes), commands/environment, complete Go test events, source capture, actual executable, reviewer and inventories are retained. The canonical proof contains 7,290 members / 145,416,827 compressed bytes / 6 parts. Every member, hardlink contents, unchanged input, part and concatenated archive SHA verifies. Concatenate parts in manifest order and verify the canonical SHA before extraction. See `independent-review.json` and `manifest.json`. No original evidence was deleted or reopened.
