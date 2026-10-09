# Current /tmp audit

Privileged inspection found only about 6.8 MiB in `/tmp`, including 4.5 MiB of Claude state, about 1 MiB of historical model sources, and the original user attachment. There are no large closed fixtures to offload. Only empty Go build directories with no process, environment, descriptor, container, loop device or mount references were removed. No file content was deleted.

The root filesystem has about 47 GiB free. Larger allocations are outside `/tmp`: approximately 30 GiB of repository Git data, 22 GiB of Go build cache, and multiple 2 GiB qualification checkouts. They were not modified by this audit.

See [audit.json](audit.json) for exact usage and removal records.
