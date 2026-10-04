# Journal network workflow context correction

The manually dispatched network proof workflow was invalid: job-level `env`
cannot use `runner.temp`. GitHub records validation failures even on pushes,
despite this file declaring only `workflow_dispatch`. Retained run 37167264632
at exact `66b7323` is terminal failed with **zero jobs**, so no workload executed.
These failures do not invalidate the independently accepted local 1,000-case
TCP proof, and do not count as real hosted test failures or successful evidence.

Move `WF_JOURNAL_NETWORK_ACK_REPORT` to the execution step's `env`, where the
runner context is supported by [GitHub's documented context availability](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#context-availability).
The report directory, workload, source capture and upload remain the same.

Official release **actionlint v1.7.12** independently rejects the exact old file
with `context "runner" is not allowed here` and accepts the corrected file.
The release archive digest matches the GitHub-published SHA256; archive/binary
hashes, complete old/new files, validation logs and original failed-run metadata
are retained here. The tool binary/archive remain in RAM; they are not included
in this directory. External shellcheck/pyflakes are disabled for this focused
workflow/context validation.

This corrects workflow validity only. No additional hosted workload is dispatched
or qualified by the static check. The original accepted local network proof and
full Tier2/Tier3/24-hour scope remain unchanged.
