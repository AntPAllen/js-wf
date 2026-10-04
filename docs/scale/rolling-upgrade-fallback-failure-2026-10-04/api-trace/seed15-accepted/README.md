# Accepted traced rolling-upgrade diagnostic — seed15

[Run37194088941](https://github.com/AntPAllen/js-wf/actions/runs/37194088941), job111413132310, executed exact `520316e04bb3b7174fd013817d375f5f6135a44a`: one full600s seed, five containers, original SIGKILL upgrade profile, forced Start-gap cut and opt-in provisioning API trace. Native reviewer verifies1848 completed invocations,20355 journal entries, all five retained-store upgrades and six completed-cohort audits. Mandatory final whole-state assertions remain attributed to the exact executed named test. Worst per-type terminal/progress p99 is13.072342745/13.046293003s, both below the executed R5 gates of30s.

The actual retained history-model executable passes Start, Signal and Await for2381 operations. All45 Go/module dependency inputs match executed source before/after. All801 selected local producer inputs match exact Git source and are retained as bytes.

Independent API review finds24 request/response pairs in each fallback check:120 requests and120 responses, no unanswered requests or API errors. Provisioning takes22–35ms across the five upgrades, beginning with `$JS.API.STREAM.INFO.WF_RUN` and ending with `$JS.API.STREAM.INFO.OBJ_WF_BLOB`. The earlier timeout is not reproduced; its blocked request and cause remain unconfirmed.

Both uploaded artifacts were downloaded completely. Original archive is40,574,164 bytes, SHA256 `d45320eec1f02f4d8fc3c9aa5f64fe1672c1deaee25f22c5058008c6c56eb2bd`; all3892 original members /143,673,926 bytes match their manifest hashes. Actual NATS executables and original stores are retained; originals were not independently reopened. The actual SDK workload executable was not uploaded by this older run and is not retrofilled from the later retention control.

`manifest.json` binds1048 proof members, the51,603,904-byte canonical archive and two parts. Every member and concatenated part readback verifies. The archive includes complete raw/API/run/job/artifact evidence, source bytes, exact reviewers, their outputs/commands, actual model executable/dependencies and canonical original stores. Restore by concatenating parts in manifest order, checking archive SHA256 and safely extracting into a fresh directory.

This qualifies this focused seed at520316e. It does not repair the failed79915ca parent, establish the historical server-side cause, qualify200 seeds or the full current-source matrix, or satisfy the actual24h soak.
