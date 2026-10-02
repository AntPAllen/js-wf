# Membership renewal failure identified

[Diagnostic run36956035359](https://github.com/AntPAllen/js-wf/actions/runs/36956035359),
source073bc01, fails64.23s into the test during the first journal-leader cut.
`fleet-failures.json` records membership/tier3-mixed-4 at02:47:59.970038343Z:
`workflow lease was lost: nats: no response from stream`. Secondary result
cancellation and interrupted WF_RUN replica-heal checks follow. This identifies
the controller's failed renewal; it does not prove whether its KV update
committed or establish a NATS server root cause. No final gate is available.

The controller intentionally stops after an uncertain renewal. The fixture
previously escalated that one member's error into cancellation of the entire
fleet. Recovery now supervises each registration and its own worker loops, joins
the stopped workers, resolves only its own epoch's leases, and registers a fresh
epoch before restarting. Strict lease.Renew and coordinator claim/CAS semantics
remain intact. All original artifacts, full job log and run API are gzip-retained
and their uncompressed hashes/sizes verified. Native acceptance of the new
supervision path remains open until the corrected sustained campaign is reviewed.
