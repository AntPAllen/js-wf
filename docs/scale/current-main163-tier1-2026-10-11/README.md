# Full current163 qualification — 2026-10-11

Source d771ee3d05fd135ec7ec81893cee3522b3499d4e includes the SDK child outcome
admission fixes and 163 seeded families/946 pins. A clean 120 MiB sparse checkout
independently matches all 3813 required Git inputs, with 3848 selected files and
exact sparse patterns recorded. Historical non-input media is excluded.

Full normal1000 launched at 07:13:06 UTC, bound to loaded user unit
js-wf-main163-tier1-normal-20261011-prepared.service, InvocationID
27a378771800445e988551b1322b3243, supervisor 273841 and driver 273864.
normal-launch.json binds the loaded command, service configuration and immutable
supervisor hash. The service uses a half-core quota, 2 GiB memory limit,
GOMAXPROCS=2/GOMEMLIMIT=512MiB within the driver, eight sequential disjoint
package processes sharing one retained executable, and 300-minute per-process
watchdogs. The 145200-second service lifetime is an engineering watchdog.

No family is filtered: acceptance requires the complete compiled inventory,
163,000 completed bodies and all 946 pins. The actual live review refuses with
exit 1. Normal acceptance is pending. A same-source race may launch only after
independent normal acceptance. No earlier campaign was restarted or replaced.
Frozen older campaigns retain their own source scopes.

Terminal review uses qualification-review.py normal, against the same retained
unit, exact Git inputs, loaded launch receipt, binary, all eight real command
exits and an independent complete reduction. Original native scale/fault/soak/
retention/import/rollout gates remain separate. Public continuation admission
remains off.
