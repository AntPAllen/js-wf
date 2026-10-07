# Explicit official RC2 lease component prepared

The bare R3 lease partition producer now accepts `--server-profile official-release --official-server-version v2.15.1-RC.2` together. The default upstream and experimental strict/contiguous profiles remain distinct. Invalid partial selections reject before creating a fixture.

The selected official source is copied unchanged from the Go-checksummed module, bound to the committed official annotated-tag provenance and exact terminal commit d564fd6982a44cc47c4228b12f7a9b6c9f722a8c. Main is built from that copy with readonly module files and no overlay; every selected dependency and copied source inventory is preserved before/after. Advertised server version and actual three live server executable hashes must match. Helper admission now uses stable live process observations. Production go.mod/go.sum remain unchanged.

Next: execute the same fresh-key/six-writer production12s TTL/one-minute markers/R3 file storage/10s isolation/debug profile and unchanged35s whole-cut recovery bound. This evaluates the official release against the observed lease component failure. No native recovery, dependency adoption, full workflow matrix, causal Tier1, original million physical-drain or24h verdict is inferred from preparation.
