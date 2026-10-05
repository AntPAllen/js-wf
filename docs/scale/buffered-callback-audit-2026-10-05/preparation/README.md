# Bounded buffered callback candidate preparation

Explicit scanBufferedConsumeByteBoundedThrough reuses the unchanged shared
invariant/bounds/gap/replay/two-resume scanner and callback cleanup lifecycle.
Defaults and the previous unbuffered candidate remain unchanged.

The SDK8MiB pull buffer is retained. The additional queue has256 record slots
and1MiB payload accounting (not header/object/RSS accounting), including the
current callback's reserved payload. One oversized record remains deliverable
when no other record is reserved. The active adapter/visitor record is outside
queue accounting. Stop wakes both byte reservation and record-channel pressure.

Unit race controls check credit release, oversized payload delivery, record/byte
pressure shutdown, invalid bounds and prior callback lifecycle. Native100k
measurement adds unbuffered and buffered callback adapters to the existing
Next/adapter/Consume/Nextrecheck comparison, validating every coordinate/payload.
Allocation totals include linked in-process R3 server activity.

No buffered native correctness/capacity/default/24h acceptance is claimed yet.
The normal measurement keeps the prior2-core/GOGC100/2GiB profile.
