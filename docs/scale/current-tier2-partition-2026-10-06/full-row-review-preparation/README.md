# Full 200-seed row reviewer preparation

`collect-tier2-row.py` collects an actually terminal successful selected-row run using public GitHub REST metadata, all201 job records, all400 raw/source artifact records, complete job logs and provider ZIP bodies. Source/run/name/duration/range substitution and incomplete paginated listings reject. Collection is fresh and read-only; queued/failed runs do not create a successful collection or authorize a restart. Partial downloads remain unqualified.

`review-tier2-row.py` requires all seeds1–200 exactly once. It independently checks every provider ZIP digest/member against extracted files and every source selection against the complete recorded Git workload selection (all non-docs files plus Go/Python/workflows in docs). It then invokes the existing raw shard reviewer for every seed: named original10m execution, nineteen scheduled faults, partition isolation/majority commits, exact raw latency recomputation and all three whole-history models. Precomputed green shard summaries cannot replace raw review. Models must match the executed dependency inputs. The whole collection must remain unchanged.

Six new control groups and eight existing shard groups pass. The actual historical partition provider artifact11153871883 matches all five original members/digest, with original bytes unchanged. New campaign sourcefcf2e94 selects1893 inputs/28040118 bytes. The actual queued37486948256 run is refused before output creation. These validate tools; they do not qualify any new native seed or full row. See `validation.json`.

After the existing run actually completes successfully, use fresh output paths:

```bash
python3 scripts/collect-tier2-row.py \
  --root /tmp/js-wf-tier2-partition200-terminal-provider-37486948256-20261006 \
  --run-id 37486948256 \
  --revision fcf2e940ee22f2dda7116d1d860683bcaa3bc329 --row partition
python3 scripts/review-tier2-row.py \
  --root /tmp/js-wf-tier2-partition200-terminal-provider-37486948256-20261006 \
  --run-id 37486948256 \
  --revision fcf2e940ee22f2dda7116d1d860683bcaa3bc329 --row partition \
  --output /tmp/js-wf-tier2-partition200-independent-review-37486948256-20261006.json
```

If model dependencies have changed in the working repository, supply `--model-root` pointing to a clean recorded-source checkout; the dependency check still applies. Do not rerun native workloads to repair unavailable evidence. Preserve complete collection/review proof and verified S3 receipt before deleting data.

Acceptance scope is one complete recorded-source selected row. Full13-row/current-source matrix, unavailable captured native workload binary/store proofs, Tier3 and actual24h do not qualify from this command. The existing read-only observer and original hosted run remain unchanged.
