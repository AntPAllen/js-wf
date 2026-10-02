# Independently reviewed admitted canonical behind-clock smoke

[Run36962797494](https://github.com/AntPAllen/js-wf/actions/runs/36962797494)
passes at exact source`410272fee5280abc4551f73475d7f33c5e4ea738` with required
pending-Sleep admission and the shared five-probe clock. All current row,
controller, clock-role, topology, pending-cut, repair and fencing reviewers pass.
It completes168 invocations,1,853 entries and144 audited waits. Worst per-type
terminal/progress p99 are5.604200392s /8.522181225s. Physical stream and all
consumer drain, immutable results, histories and retained invariant gates pass.
Source removal04:07:59.083086247Z precedes the selected earliest duration
boundary04:08:00.591157254Z by1.508071007s. All31 observed repairs are acknowledged
(three start,23 signal,five suspended); no fencing events occur.

The whole child-start budget is exercised by actual missing enqueue replies:
two calls return`invocation stored but run enqueue is unconfirmed` after
5.000725632s and5.000382092s, rather than holding their deliveries indefinitely.
The same parents retry with matching-existing-child responses and subsequently
complete. A complete correlation retains all their operation events and
independent parent journal receipts. It proves bounded uncertain outcomes and
successful replay, without assigning a broker-side cause to the missing replies.

All74 original downloaded artifact/API/full-job/review/correlation files are
compressed and verified against uncompressed hashes. This clears one admitted
35s seed; it does not clear the failed historical source, sustained19-cut
admission,200 seeds, ahead physical drain or24-hour/full-release scope.
