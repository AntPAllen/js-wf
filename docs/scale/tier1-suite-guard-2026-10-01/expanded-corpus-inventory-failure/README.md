# Expanded corpus packaging failure

At sourcee25b881a406ef72cc616bd8e209d840c11953ad7, the complete1,000-seed
simulator suite passes110.246s:150 top-level passes,two expected trace-only
skips and180 pinned traces. The suite-result guard rejects the source
regression inventory because four new timer-clock trace filenames contain
colons, outside its portable filename contract. Unit js-wf-tier1-current-20261001
therefore ends failed. This is not certified comprehensive-suite evidence.

Original events,inventories,source,output,time and guard error are retained.
Large files are deterministic gzip; hashes refer to uncompressed bytes.
The fix renames the four pins with hyphens and updates their generator.
The strict guard is unchanged. A fresh complete suite is required after fixing.
