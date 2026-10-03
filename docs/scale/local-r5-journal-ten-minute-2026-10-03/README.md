# Current-source five-node journal row: retained ten-minute race execution

Source: `1f1abfc9ea1794a6e0d13af156bb1cbd4a516194`.
This includes all six scanner prefix corrections and physical tombstone marker
cleanup. The producer uses an isolated detached checkout and disk-backed stores.
It is terminal with `row_verified`, test exit 0, and no remaining Docker nodes.

Independent review accepts the exact requested single row:

- 714.110 s named test / 715.152 s package;
- full 600-second workload duration, 85 batches / 2,380 invocations;
- 26,217 audited journal entries and 19 confirmed journal-leader faults;
- worst per-workflow terminal/progress p99: 14.018474603 s / 7.048139213 s;
- 5,000 archive members, including all physical stores and compiled checkout;
- all 1,140 selected before/after source hashes match the exact Git tree;
- actual race executable SHA256
  `e3481cd65c318f7f205ae120ca5a437b0626f80aab37239f65a7910cfd48cea9`;
- byte-identical raw report regeneration, checkpoint audits, event explanations
  and review of all 21 fencing records.

The independent reviewer hash-checks physical stores but does not independently
reopen them. This qualification covers one journal fault row/seed for ten
minutes. It does not clear the whole fault matrix, 200 seeds, or 24-hour gates.

## Storage observations

The cluster's regular-file allocation was 107,732,992 bytes at about 2m44s,
115,445,760 at about 5m17s, and 166,961,152 at about 9m09s. After shutdown it fell
to 127,594,496 bytes. Transient files and allocation changes mean the observed
live growth is not a steady-state capacity proof. Linear extrapolation of the
first-to-last live interval yields roughly 13.35 GB/day; that is an estimate,
not a lower bound or predicted actual final size. Root free space after the run
was about 1.87 GB. A 24-hour disk-backed row remains unqualified and capacity is
not established on this 35 GiB disk. No shortened profile substitutes for it.

Raw observations are retained, including timestamps and logical/allocated sizes.

## Archive

The unchanged producer archive is split into ordered 25 MiB parts. Each part
and the full archive SHA256 are recorded. Concatenated part bytes were verified
against the original archive; every one of its 5,000 members was reopened and
hash-checked again before publication.

```sh
cat originals.tar.gz.part-* > originals.tar.gz
tar -xzf originals.tar.gz -C /path/to/fresh-root
python3 scripts/review-tier3-soak.py --root /path/to/fresh-root --repo /path/to/repository --output /tmp/independent-review.json
```

Copy the provided `archive-manifest.json` into the extracted root for review.
The recorded source/verification scripts are inside the archive. Metadata does
not authorize running archived workloads during review.
