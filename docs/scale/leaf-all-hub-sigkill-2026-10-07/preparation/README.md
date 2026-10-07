# All-hub and leaf process SIGKILL prepared

The new `leaf-retirement-all-sigkill-expiry-weak` profile keeps the original strict retirement/reuse scenario, 60-second scenario deadline and 30-second whole-cut gate. All three WFRETIRE hubs and the WFEDGE leaf are separate module-pinned stock NATS processes. Every original hub must be SIGKILLed and reaped before any replacement starts. The leaf must also be SIGKILLed/reaped, all three runtime clients must disconnect, and the physical outage must exceed production TTL12s. The selected fresh frame still receives one controlled weak absence and exact real remote-domain leader confirmation.

New process leaf listeners retain their original ports/configuration/routes/store through restart. Documented `/jsz` snapshots admit the three-member metadata group and are captured before workflow placement. Every original/replacement process identity and executable must be observed; replacement argv and hashes must match. Existing graceful hub profiles remain distinct.

Compilation passed for `./testcluster ./integration`. Existing leaf proof controls and domain log controls passed; a renamed graceful-hub log must fail all-hub qualification. Native execution and independent complete archive review remain pending. No natural follower-lag, daemon-child transport or broad release qualification follows from preparation.
