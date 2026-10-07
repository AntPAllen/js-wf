# Failed normal100k launches

The first root-owned service failed Git ownership validation before building or running a test. The second service used `exedev`, built the exact `3a7022c` binary, but its sample trace output directory was absent. The actual test failed immediately while writing the first sample; completed seed bodies were zero and stable live SDK admission was not obtained. Its three pinned replay subtests passed, which does not qualify the seeded workload.

Both original failure logs are preserved. The complete unchanged second fixture is captured with full member/current-inventory/closure verification; this storage proof does not repair its verdict or establish actual SDK admission. A fresh v2 fixture creates the sample directory before launch and retains the same source/binary and 100,000-seed target. See [live admission](../normal100k-launch/README.md); its terminal result remains separate.

The completed local fixture and staging archive are now retired after fresh remote/member/current-inventory/closure checks. Complete S3 receipts remain here; [retirement and restoration](../seeded-reclaimed/README.md).
