# Retained diagnostics for the unresolved continuation/promise boundary

The earlier combined native continuation/promise test failed at `after_manifest`; its runner did not upload original stores or the actual executable. The snapshot first-lookup timeout error-cause correction is already qualified by full Tier1 normal100k/race1k, but it does not establish a fix for the original server-side cause. See [the unchanged failure bounds](../snapshot-timeout-cause-2026-10-04/).

`WF_PROMISE_ARTIFACT_PARENT` now opts this integration fixture into a fresh retained directory per cut. Original server stores/configs/logs, worker output, effect markers and staged promise/frame files survive normal test cleanup. A JSON report is written after child/server cleanup and records actual verdict plus wall/monotonic timing and primary error/deadline classification around the pre-kill journal/checkpoint reads. That pinpoints the original pre-kill read boundary without replacing its error, retrying it or changing any timeout or recovery gate. Default tests keep their existing temporary-directory lifecycle; production runtime/simulation/Tier1 producer code is unchanged.

The integration package compiles and the disabled named profile returns in0.035s; this is only preparation, not a successful native cut. A focused diagnostic must still run against a captured actual executable at the pushed prepared source, retain complete Go test events/pre-post compiled sources and all original files, and independently review its outcome. Success at one cut cannot qualify all eight combined cuts or repair the failed historical CI parent. Original90s overall context,25s server readiness and30s worker recovery bounds remain unchanged.

```sh
WF_PROMISE_FULL_RESTART=1 \
WF_PROMISE_ARTIFACT_PARENT=/tmp/promise-diagnostic-originals \
go test ./integration \
  -run '^TestContinuationPromiseSIGKILLAndFullServerRestart$/^after_manifest$' \
  -count=1 -timeout=5m
```

For actual proof capture, compile once to a retained executable first and use `go tool test2json` with the same subtest pattern; the simple command above does not itself preserve a Go temporary executable or source provenance. Do not infer an original missing/corrupt snapshot or NATS bug from a deadline alone. The JSON report captures stopped-state originals; it does not independently reopen stores or prove their state at the earlier failed lookup.
