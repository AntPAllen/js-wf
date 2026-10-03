# Hosted pressure diagnostic fails before consumer/probe load

[Run 37115026852](https://github.com/AntPAllen/js-wf/actions/runs/37115026852)
at `0b69e91dce0514c37e83ca855eb3686edca64ed7` fails its actual named
`TestLeaseAppendPressureConsumerTrafficFixedPlacement` race test in 25.06 s.
This result is rejected and does not qualify the complete six-row diagnostic.

The uploaded report contains three healthy rows and one delayed baseline row.
In that fourth row, owner `dtrue-atrue-w8-1` completes twelve mandatory renewals,
then receives:

```text
append: journal append outcome unknown: tail lookup: nats: API error: code=503 err_code=10008 description=JetStream system temporarily unavailable
```

The other seven owners each complete 48 renewals. The failed row has neither
consumer traffic nor held-acquisition probes. It therefore cannot establish
that either load caused this error. The three healthy rows report 48 renewals
per owner; their summaries are retained without promoting this incomplete test
to an accepted comparison.

Final monitoring sees the expected lease/state leader on node0 and journal
leader on node1, with the healthy surviving peer current. Node2 refuses its
monitoring connection, matching the fixture's stopped-node condition. Those
later snapshots do not establish server leadership or internal state at the
earlier failing tail request. No underlying server cause is confirmed.

The pinned journal source returns this `ErrUnknown` at its pre-publication tail
lookup. The fixture deliberately stops on any operation error; it does not run
the worker's redelivery/recovery path. This is an incomplete steady-pressure
measurement, not evidence that the runtime acknowledged a lost journal append.

All twelve archive members were SHA256 checked by readback before atomic
publication: original uploaded summaries, disk trace, final monitoring, terminal
metadata, failed-job log, review, and three selected files obtained from exact
Git source. There is no complete producer source inventory or physical store
archive for this job. No fresh duplicate trial was launched.
