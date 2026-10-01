# Worker continuation retirement, GC and ID reuse

The source manifest records the tested production code, model, adapter and
real fixture. Production code is unchanged from a8064ac/cebd561. This adds a
worker-integrated vertical slice to the SDK continuation retirement model.

## Shared transport and protocol

Production client Start, worker dispatch/lease/journal/checkpoint decisions,
retention and quiescent GC use the same retained invocation, signal and journal
maps, WF_STATE KV, Object Store and lease store. Only run delivery/dedup uses
the established WorkerTransport. Invocation publish enforces the production CAS
header atomically on the shared map; no post-hoc copying connects stores.
Signals are fixture publications with explicit invocation generation, consumed
by the production worker; the full client Signal API is not modeled here.

For each schedule, input 23 runs a prefix effect, saves SDK state/locals and
continues to finish_v1, then suspends for a signal. Purge must reject the
nonterminal invocation. GC with workers stopped preserves both frame/archive.
A replacement worker consumes the enabling signal, restores state/locals,
executes the suffix effect and publishes terminal result 46. Replacement workers
must not read archive objects; initial checkpoint publication and subsequent
retirement audits may verify archives normally.

Seven modes cover clean retirement, dropped journal purge, dropped snapshot
manifest deletion, lost tombstone reply, lost purge-event reply, lost invocation
purge reply, and lost reply to a committed reused Start. GC runs between an
uncertain retirement attempt and retry, then after completed retirement. The
retired frame/archive are reclaimed. Tombstones match the retired generation.

The same ID starts with input 77 and a strictly higher generation. A late old-
generation signal with payload 999 must leave the fresh continuation suspended.
Fresh-frame GC preserves its two objects. A second replacement worker consumes
only the current signal and returns 154. Four effect callbacks have four unique
RunOnce keys across stages/generations. Each terminal passes the raw retained
journal/state checker, including Started at index zero and outcome identity.
Final retirement reclaims the new frame/archive, retains exactly two deduplicated
purge events, and requires the fresh generation's tombstone, no invocation,
no snapshot manifest and no purging marker.

## Verification

- Final-source 100,000 schedules PASS 176.760 seconds, 45,468,707 transport
  events, 100,000 choices, maximum virtual time zero. This is response-fault
  coverage, not lease-expiry timing, process/network partitions or a soak.
- Exact first-ten replay and separate-process seed-42 byte identity pass. New
  pin and complete corpus/workload race PASS 34.007 seconds (workload 31.34).
  Existing pins remain unchanged. Vet passes.
- Matching real R3 retirement/GC/generation-reuse race PASS 40.912 seconds,
  covering clean and lost-manifest publication cases. Its additional child/
  shared-object scenario uses different data; this is structural integration
  evidence, not byte-identical differential traces of all seven model faults.
- Compiled snapshot-manifest deletion omission fails with two objects still
  referenced and zero reclaimed (0.005 seconds). Omitted invocation generation
  in RunOnce keys fails with reused effect keys (0.007 seconds). A tombstone
  forced to generation 1 passes the old generation but fails the fresh final
  tombstone assertion (0.012 seconds). Logs and reproducible patches are retained;
  build errors, unrelated timeouts and skips are not controls.

This fixture uses small inline input/results and one invocation at a time.
The separate SDK model proves shared frame-held promise references. Full
worker-integrated shared-promise/retirement cases, combined process/server cuts,
active-writer coordinated GC, TTL timing, final-source full Tier 1 campaign and
independent full matrix/24-hour release gates remain open.

## Hosted runs observed during this work

cebd561 standard workflow passed; its 20 mixed seeds and lease-disk contract
passed, while its independent pressure job failed at a stopped metadata leader
before disk delay. That failure is retained in the previous pressure proof.
The [a8064ac mixed workflow](https://github.com/AntPAllen/js-wf/actions/runs/36826882488)
passes all 20 seeds and both pressure/lease-disk jobs. Its full mixed log and job
states are retained here; pressure artifacts are retained in the sibling proof.
Neither 20-seed campaign clears the 200-seed whole-matrix gate or explains the
prior ~29-second KV-update observation. Later full simulation jobs and the
original million-timer process continue without restart.
