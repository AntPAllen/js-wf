# Rolling-upgrade seed8: lease-bucket provisioning timeout

[Run37164231641](https://github.com/AntPAllen/js-wf/actions/runs/37164231641), failed job111324440201 /rolling_upgrade1–13, executes exact `79915ca41a5c5a23b9997eea5f3f66d82530ee30`. Seeds1–7 have producer passes only; they are not independently qualified here. Seed8 fails at fault4;9–13 do not execute. Complete raw artifact11301141635 /8,411,370 ZIP bytes and original artifact11300567436 /323,029,835 ZIP bytes were downloaded.

Version4 after-upgrade proof completes five native semantic rejections, then fallback provisioning returns `bucket WF_LEASE: context deadline exceeded`. Backend operation starts10:33:02.831076493Z and ends10:34:02.466498027Z against the original whole-proof deadline10:34:02.465617160Z. At this point node3 remains2.11.17 and the other four peers report2.15.0. Earlier final peer health snapshots all return200; this does not prove availability throughout the operation.

The executed provisioning source places the wrapped bucket error in the KeyValue lookup/create/retry path, before bucket Status validation. That identifies a phase, not the exact blocked API request or server-side cause: the old run has no request trace. No deadline/retry changes, historical-cause closure or runtime repair are claimed.

Independent review verifies all eight unchanged pre/post source captures against executed Git;759 selected local source input bytes are retained. Canonical original archive322,754,362 bytes /SHA256 `64f842c1077946c4510e30b80b8a4a51d3d481b5caf1d9303d0a6ef6d31e569c`; all31209 member contents /1,114,967,818 bytes match the original manifest. Actual NATS executables and original stores are retained; stores were not independently reopened. Actual SDK workload executable was not uploaded and is not retrofilled.

Proof:1531 members /332,448,704 compressed bytes /13 parts, all member contents, unchanged inputs and concatenated part readbacks verified. Manifest binds every member/part/canonical hash. Concatenate in listed order, check canonical SHA256 and safely extract into a fresh directory. Complete raw/run/job/artifact/log/source/reviewer evidence and canonical original stores are included.

This preserves a failed shard. Full matrices, historical parent/cause and actual24h soak remain open. Current opt-in instrumentation traces both native and fallback API calls and retains the actual SDK for future focused diagnostics; seed41 diagnostic37197193507 remains the existing run to observe.
