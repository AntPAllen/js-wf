# Controller journal receipt and append-window proof

Four focused tests passed under the race detector, package4.379s (Go output4.377s). One boots an actual three-node cluster, starts an ordered journal consumer before a real production append, validates receipt identity against the retained entry and uses its unshifted read timestamp to bound an otherwise unknown append. The unknown status in this contract check is synthetic; it is not an injected real lost acknowledgement. The successful append and independent receipt are real.

Controls reject wrong owner/index/kind/invocation, failed ordinary appends, missing or invalid timing, changed receipt epoch/payload, duplicate receipts and missing receipt times. A timeout return alone cannot establish a commit upper bound: the server may commit later. The pure control explicitly permits a later independent receipt to bound that case. Causal stream sequence chooses progress even when earlier acknowledgements arrive later than subsequent ones.

The original Go JSON stream and source hashes are retained. Integration/testcluster vet and diff checks pass. These helpers are not yet connected to the sustained peer-clock rows. Those rows remain fail-closed pending controller observations of timer and enabling events, the end-to-end latency audit and actual clock/fault execution. No release gate is cleared here.
