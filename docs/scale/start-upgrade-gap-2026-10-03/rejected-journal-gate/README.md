# Rejected immediate journal predicate

Clean source1b03266b23da7729e48800910279e3cc97221a06 records an actual positive
package failure37.372s: old-peer-first rejects the absence of a journal within
two seconds of successful scanner repair; auto-fallback-on-new-peer passes.
The precise omitted-repair control did not execute because the positive failed.

This predicate did not inspect retained WF_RUN. A successful repair publication
and worker journal processing are separate protocol events. No contemporaneous
queue cut was recorded; the earlier worker processing delay's cause remains
unconfirmed. This is not evidence of a runtime or NATS defect.

The correction checks for the actual retained matching dispatch or journal
progression, preserving a semantic failure for an omitted repair, and separately
enforces terminal completion within30s of confirmed child SIGKILL. No failed
run is promoted by changing its offline interpretation.

All before/after source hashes match the recorded Git source. The archive retains
the actual race binary/build metadata, Go JSON, commands, precise original
source, markers, process proof and original physical stores for both profiles.
Every member SHA256 is checked by reopening the temporary archive before atomic
rename. Full R5 mixed gap coverage, matrices and24h remain open.
