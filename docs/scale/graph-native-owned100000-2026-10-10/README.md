# Native 100,000-owned-grant diagnostic — 2026-10-10

The preceding 10,000-orphan R1 test logs complete renewal and audit of 10,006
grants across 79 fresh batches, with recovery plus renewal 4m46.723596664s and
maximum batch 4.391456815s. Its transient service unloaded before terminal exit
capture, so that run remains diagnostic rather than accepted service evidence.
It is retained; no rerun is needed merely to recover that missing bookkeeping.

Linear extrapolation of its measured renewal cost to 100,000 grants is roughly
48 minutes, exceeding the 20-minute renewal window for a one-hour intent.
This estimate motivates an explicit three-hour fixture lifetime, with a one-hour
renewal window. It is not a prediction or acceptance claim. The experiment will
check whether native cardinality or batching invalidates it.

The fixture now accepts WF_GRAPH_NATIVE_COMPACTION_LIFETIME (1h..6h). Default
remains one hour; production GraphConfig defaults remain unchanged. For this
diagnostic, original and target lifetimes are three hours, the complete fixture
watchdog is 90 minutes, the Go watchdog 100 minutes and service watchdog 110
minutes. Setup stays 3 seconds and each 128-scope renewal batch 15 seconds.
The measured interrupted renewal, peer restart, fresh reconstruction and full
scan must fit one hour and the remaining original expiry. Provisioning and final
audit also must fit the fixture watchdog.

All 100,000 added scopes are native CAS-created uploading grants, not just owner
markers. Six small-compaction grants and one absent boundary are also checked.
The same committed-CAS/lost-acknowledgment interruption, all-peer same-store
restart, portable reconstruction, full grant audit, unchanged source head and
independent final publication remain required.

This experiment uses a four-record source. Actual 100,000-entry qualification,
R3 large renewal, worker/process/VM/storage recovery, and original broad gates
remain open. A longer intent retains abandoned grants for longer; no production
budget, admission or online collection is selected by this test.

supervisor.py atomically saves the actual Go exit before returning its same
status. The service uses RemainAfterExit=yes to preserve its invocation and
ExecMainStatus. Acceptance requires both terminal records, matching launch and
source fingerprints, and complete test output. A launch is not acceptance.

## Launch

Source 24dd8af, R1, started 2026-10-10 21:48:30 UTC. The retained service is
js-wf-native-owned100000-20261010.service with invocation
3c0c89936f9f479c86467f9a8bc21c95. launch.json records the exact budgets and
configuration; process-state.json records the actual child. Run review.py to
observe live status or verify terminal completion. No acceptance is implied by
this launch.
