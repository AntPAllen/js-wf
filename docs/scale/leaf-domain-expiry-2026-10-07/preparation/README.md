# Leaf SIGKILL plus production lease-expiry profile prepared

Explicit `leaf-retirement-sigkill-lease-expiry` retains the complete strict retirement/reuse scenario and actual leaf SIGKILL/client disconnect/hub restart checks. Before stopping peers it admits actual WF_LEASE TTL12s, current held owner/epoch/revision. After all hubs and the leaf are stopped, it holds13s and then heals under the original cut-start-plus30s deadline. Terminal journal must be Completed at a strictly higher epoch than the observed old owner. Original60s scenario/3m race SDK/2CPU/1GiB and all object/generation/effect/count checks remain.

Compilation, two prior actual-proof control groups and12 existing domain verifier groups pass. No actual native expiry acceptance yet. CI retains all three leaf profiles. Full current-source matrices, all-hub SIGKILL/packaged daemon leaf cuts, onlineGC, million physical drain and actual24h remain separate. Existing24h SDK is isolated atbc9f92b and remains live.
