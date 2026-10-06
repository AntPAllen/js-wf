# Built worker CLI controls

Preparation: actual `wf-worker` process execution of the complete existing default/domain static, KV and auto workflow/metrics/capacity/tombstone/retention/purge/reuse fixtures, plus startup while all real embedded NATS peers are down followed by library restart with fresh IDs. Same original35s case contexts and4m race SDK. Eight real children receive SIGTERM and must join with exit0.

Run from clean main:

    python3 scripts/run-worker-domain-controls.py --case standalone-commands --root /tmp/js-wf-worker-standalone-commands-20261006

The runner retains actual live `/proc` birth/argv/executable digests, compiled VCS identity, stdout/stderr and exit records. Parent domain admission is checked against actual peers. Child outgoing API prefixes are not traced; this does not qualify native server SIGKILL, leaf routes, fault matrix or full release. Native execution and independent review remain pending.
