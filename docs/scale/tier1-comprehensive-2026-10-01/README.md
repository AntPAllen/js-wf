# Comprehensive deterministic campaign

[Run 36900782095](https://github.com/AntPAllen/js-wf/actions/runs/36900782095) succeeded at `431c555d092bc30dec84b7ceb23477e566fc2233`.

The original seeded-suite job log is retained compressed, with its uncompressed SHA-256 and aggregate counters in `proof.json`. There were 144 top-level passes and two expected skips: replay and minimization require an explicit `FAULT_TRACE`. The configured seed count was 100,000 per seeded workload; aggregate scheduler counters do not independently establish each workload's seed coverage.

The package passed in 12,939.618 seconds, generating 9,900,632 schedules, 172,856,738 scheduler choices and 2,328,341,361 transport events. No success artifacts were uploaded by that workflow version; this evidence comes from its original GitHub job log.

This source predates the parent-notification attempt budget and later repair/fencing observation changes. It is a verified campaign for its recorded source, not the final-source release gate. A new comprehensive run is required for those changes.

## Expanded source campaign

[Run36940808396](https://github.com/AntPAllen/js-wf/actions/runs/36940808396) launches at clean source `faf92045d88933090d2341e530df54f513bf396a` with100,000 configured seeds per workload,expected152 compiled tests and180 pinned traces. This workflow includes the strict full-suite evidence guard and always-uploaded source/inventory/events/results. It is authoritatively queued at launch and has no acceptance yet. Earlier36929826425 remains running; it was not cancelled or replaced.
