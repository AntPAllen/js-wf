# Original 100,000 distinct-ID Start count proof under continuous route faults

At exact **`3b859aa6f7385b09792b6985156e8ef880108f27`**, the actual real three-node R3 file-store `TestDistinctStartsUnderRoutePartitions` passes **44.17 s** (wall **44.416 s**). All96 producers complete **100,000 distinct IDs**, and original named assertions verify **100,000 invocation messages, 100,000 invocation subjects and 100,000 run messages** after confirmed final route healing.

The retained timeline independently verifies **206 alternating route changes**, including **103 confirmed isolations** with node2 routes0 and an intact majority route. The last change observes **99,994 completed starts**, **41.000998 s** after producer work began, versus total producer duration **41.103117 s**. Scheduled200 ms toggles therefore span the complete workload rather than only its first2.4 s. Median/maximum observed change-request interval is **0.200014/0.202080 s**. Healing events record mesh-heal requests; only cut samples and the final heal assert observed route state. **403 retries** and **46 attempts beginning on the isolated node** are retained without erasing uncertain attempts.

## Exact execution and retained originals

Actual executed binary SHA256:

`3e5a354851af0d3f9b41a0621fd49bb1ca76356dd5efc053776c6cb518eae49c`

The proof retains that executable, build information/commands, report, stdout/stderr, exact runner/reviewer/preservation scripts, original dependency listing, pre/post source ledgers, captured source bytes and every original broker file. All **3,848 selected Go/Cgo/test/module inputs** match before compilation and after execution; all **561 selected local inputs** also match exact executed Git. This package inventory is a superset: it includes dependency test sources even when not linked. Assembly, embedded and compiler-generated inputs are not claimed as exhaustive hermetic compilation capture. That boundary is explicit in the independent review.

All **375 original store files**, **110,572,995 bytes**, are hashed and retained. The proof's **4,241 members** are independently read back and SHA256-verified before publication. Its two archive parts concatenate to the recorded complete **52,115,188-byte** digest. Reassemble sorted `proof.tar.gz.part-*` outside the repository, verify `manifest.json`, then extract. No physical store is rewritten or independently reopened. Stream counts retain their executed named-test assertion provenance, while the route timeline/source/executable/archive checks are independently reviewed.

This qualifies the supplied plan's **100,000 distinct-ID count proof with continuous route partitions** at this source. It does not qualify all of Phase1, the separate Start history/linearizability or lost-reply cases, current full Tier1 normal100k, complete fault matrices, the million-timer physical-drain gate or actual original24h soak. No smaller population or shortened fault period substitutes for this count proof.
