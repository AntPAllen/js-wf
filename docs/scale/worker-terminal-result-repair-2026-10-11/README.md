# Verify terminal result bytes before worker repair

The active worker now resolves a Completed external result through the bounded
legacy result port or the invocation's owned canonical graph references before
repairing WF_STATE or notifying a parent. ResultBytes rejects inconsistent
inline/reference envelopes and verifies SHA-256. Unavailable or mismatched reads
leave the terminal journal intact and the outcome unprojected; a fresh delivery
after transport recovery can complete repair. Failed outcomes do not fetch bodies.

## Evidence

- Baseline source 25f89fb, final fixture and a Go source overlay: actual exit 1;
  six unavailable/wrong-byte/conflicting-envelope projections and two healthy
  cases with no verification reads. The exact original worker source is retained.
- Final normal 0.490 s and race 5.773 s: actual exit 0, all 172 exact CI controls,
  eight new backend/mode cases and four healed fresh-worker deliveries. Handlers
  never run, valid projections match original terminal payload bytes, and both
  backends retain their original two records. Canonical physical references are
  independently checked.
- Native child and buffered-child compatibility, R1/R3Domain, race: actual exit 0,
  69.462 s; four flows, eight checkpoint frames and four verified start/terminal
  link witness pairs. All final command executables were observed with their SHA
  and build metadata. Modeled executables additionally bind their Go parent PID.
- 1153 prelaunch source/module/workflow inputs are hash-bound. Native completed
  before a test-only fixture correction that stops supplying legacy ResultBlobs
  to a canonical worker. Original input manifest/fixture and actual failed normal,
  race and baseline attempts are retained under fixture-option-failure. The
  reviewer verifies this exact correction and that all other inputs are unchanged.
- The current parsed YAML CI assertion body accepts both final local logs. This
  is local evidence; hosted CI has not been claimed.

Run `python3 docs/scale/worker-terminal-result-repair-2026-10-11/review.py`.

## Scope

Faults are deterministic result-port responses over intact fixtures, not object
deletion or a NATS server fault. This establishes active worker repair admission,
not legacy lease-held acknowledgement body verification, parent causality,
application result equivalence, full modeled worker scheduling, or all maintenance
paths. The frozen full162 campaign predates this fix. Full current qualification,
original release gates and public continuation admission remain open/off.
