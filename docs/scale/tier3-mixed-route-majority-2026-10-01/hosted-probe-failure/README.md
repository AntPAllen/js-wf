# Hosted majority probe failure: excluded acceptance

CI36914593168 atcc001d2 is terminal FAILURE. The first fault confirmed the
single-node cut but the one-shot3s probe publication returned nats: no response
from stream. The controller canceled remaining work. The named race test
failed66.84s; no final history/state/drain/latency acceptance is established.
Original workflow metadata, events, incomplete fault and all server evidence
are retained. The exact server-side cause is not established.

The probe imposed a separate3s availability threshold beyond the plan's30s
workflow p99. The fixture now uses a15s whole probe budget with the existing
named-transient retry helper. Permanent failures still stop immediately;
all workflow raw enabling-event p99/history/invariant/drain gates remain.
The separately retained smoke passes after this change, but it does not
reproduce or prove recovery from this exact hosted interleaving. A fresh
clean-source ten-minute run is still required.
