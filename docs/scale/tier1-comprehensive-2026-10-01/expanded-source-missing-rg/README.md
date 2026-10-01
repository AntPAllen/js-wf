# Comprehensive workflow setup failure

Run36940808396 at sourcefaf9204 fails before any seeded workload executes:
its runner does not provide rg. The compiled Go test listing succeeds, but
the inventory shell command exits127 with rg: command not found. No events,
coverage or simulator verdict exists. Original listing/source/log artifacts
are retained. The workflow now uses standard grep/find inventory commands,
verified byte-identical against the completed local inventory at180 pins.
The strict evidence guard is unchanged.
