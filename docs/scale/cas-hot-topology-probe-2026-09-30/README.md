# Controlled hot-journal client placement probe

The earlier paired CI gate failed with fast (~2,150/s) and slow (~1,540/s)
hot-subject samples in both baseline and candidate. Its pinned client was always
node 0, but it did not record stream leadership.

This local diagnostic uses one real three-replica file-backed cluster and nine
fresh subjects of 10,000 entries each. Three rounds rotate the first pinned node
(0/1/2, 1/2/0, 2/0/1). The leader was node 2 in every before/after snapshot.
Median leader-client throughput was 3,541.50 appends/s (three samples), versus
2,578.10 through followers (six samples), a follower/leader ratio of 0.7280.
Every leader sample exceeded every follower sample. All 90,000 messages across
nine subjects were retained. The million-timer campaign was also running on
this VM; this is a placement diagnostic, not a release throughput comparison.

This supports placement as a confounder in the old CI comparison. It cannot
attribute the old failure because those samples have no leader metadata.
Before/after snapshots also cannot exclude a transient intervening leader change.
The raw report, command output and binary/build identity are retained; the probe
used modified source before the controlled-placement gate edit.

Reproduce the diagnostic with:

```sh
go run ./cmd/wf-cas-bench -probe-hot-topology -output /tmp/topology-probe.json
```

The revised CI gate separately requires both leader and follower placements to
pass the unchanged 80% hot and parallel median thresholds. Both production
revisions use the same measurement harness, preserved in the artifacts, while
the reference journal and its dependencies remain fixed. The old failed report
remains retained. Fresh hosted acceptance is required.
