# Streaming audit native journal-fault controls

Executed source: `4616d4795300fbfcba8396d6da001760273773f1`.
Actual race SDK: `761ac7776d08c1d3b04dd9c2bb448c07a9fbd7bf3950f0e083303c3fc803bba9`.

Four fresh three-node file-backed fixtures pass. Each contains500 invocations,
2000 acknowledged journal records and500 terminal values. Both streaming modes
(point state and fresh watch snapshot state) first match the original batched
checker. During the streaming scan, the actual R3 memory consumer leader shuts
 down at callback128 with1488 records pending. Full reports recover in0.557s
and0.445s under the unchanged20s attempt deadline. Paired cancellation returns
context.Canceled at exactly128 callbacks and retains only the invocation report.
Named tests assert both temporary stream consumers clean up tozero.

The reviewer verifies all2894 captured selected inputs,64 local Git files,
runner bytes, actual binary/all Go build-info fields, named results and raw fault
rows. The complete archive contains4343 members/96301989 uncompressed bytes;
all original/member/part/concatenated digests read back. See manifest.json and
independent-review.json. Concatenate the numbered parts to reconstruct proof.tar.gz.

This is native library shutdown, not OS SIGKILL. Live process identity was not
captured; stores are retained but not reopened. State-watch delivery is not
interrupted by this test. Large remaining-tail recovery, legacy/five-container
coverage, full matrices and actual24h qualification remain separate requirements.
Default production readers and audit limits are unchanged.
