from pathlib import Path
import json,shutil
base=Path('/home/exedev/js-wf/docs/scale/current-tier3-block-delay-2026-10-04');out=base/'full200-qualification';r=json.loads((out/'coverage-qualification.json').read_text())
assert r['accepted_seeds']==200 and r['qualifies_full_row'] and not r['missing_seeds'] and r['all_archive_members_and_parts_verified'] and r['all_actual_models_and_dependencies_verified'] and len(r['shards'])==16
(base/'aggregate.json').write_text(json.dumps({k:v for k,v in r.items() if k!='model_dependency_sha256'},indent=2)+'\n')
table=['| Seeds | Job | Invocations | Entries | Faults | Model operations |','| --- | --- | ---: | ---: | ---: | ---: |']
for s in r['shards']:
 first,last=s['first'],s['last'];d=base/f'seeds-{first}-{last}';m=json.loads((d/'manifest.json').read_text())
 table.append(f"| [{first}–{last}](seeds-{first}-{last}/) | {s['job']} | {s['invocations']:,} | {s['journal_entries']:,} | {s['faults']:,} | {s['model_operations']:,} |")
 if first not in (170,183):continue
 (d/'README.md').write_text(f'''# Real disk-delay seeds{first}–{last} accepted at79915ca

Run37164231641/job{s['job']}/artifact{s['artifact']} binds thirteen complete600s
cases to79915ca41a5c5a23b9997eea5f3f66d82530ee30. Independent raw fault/admission/
report/explanation/fencing checks and all three rebuilt models qualify
{s['invocations']:,} invocations/{s['journal_entries']:,} entries/247 real faults/
{s['model_operations']:,} exactOk operations. Every writable node4 store/R5 leader/
100ms dm-delay/minimum5s interval/delayed sync/restoration verifies; all
{s['completed_cohort_audits']} expected completed-cohort audits pass. Worst terminal/
progress p99={s['worst_terminal_p99_seconds']:.9f}s/{s['worst_progress_p99_seconds']:.9f}s
under original30s gates. All45 actual local model dependencies match Git.

All{len(m['files'])} canonical members/three parts read back; compressed
{m['archive_bytes']:,} bytes, SHA256 `{m['archive_sha256']}`. Authenticated original
ZIP/raw expansion, API/job logs/transfer attempts, reviewers, model executable/
source/build info and exact outputs retained. Concatenate sorted parts and verify
manifest before extraction. Final whole-row aggregation rechecks every part/member.

Workload attribution is checkout/header binding; SDK/full captured source/native
stores unavailable. Integrity/drain native named-test scope. These executed-source
seeds qualify; full/final-source matrices/24h/million-drain remain open. Historical
failed parent campaigns remain failed; no physical reopening/causal claim.
''')
table.append(f"| Total | | {r['invocations']:,} | {r['journal_entries']:,} | {r['faults']:,} | {r['model_operations']:,} |")
(base/'README.md').write_text('''# Complete recorded-source Tier3 disk-delay row

All200 consecutive seeds qualify at79915ca in run37164231641. Sixteen successful
600s-per-seed shards pass raw actual-device/R5 leader/delay/sync/restoration,
report/explanation/fencing/cohort controls and all three independently rebuilt
history models.

'''+ '\n'.join(table)+f'''

All{r['completed_cohort_audits']} expected completed-cohort audits verify. Worst
terminal/progress p99={r['worst_terminal_p99_seconds']:.9f}s/
{r['worst_progress_p99_seconds']:.9f}s under original30s gates. Every actual model's
45 local dependencies matches executed Git. Combined reviewer reads every part/
canonical member in all16 archives and verifies exact200-seed coverage, actual
model/source identities, raw history counts/exact outputs and original600s/19
confirmed real faults/30s p99/cohort requirements.
[Complete200 qualification](full200-qualification/).

Workload source attribution is checkout/header binding. SDK/full captured workload
source/native stores unavailable; integrity/drain retains named-test scope. The
executed-source row qualifies, not final-source/full16×200/24h or million physical
drain. Historical failed parent campaigns remain failed; no reopening/causal claim.

Earlier duplicate raw expansions were recovered only after complete published/
ZIP/canonical/member verification. Retained ZIPs/proofs restore every byte.
[Recovery1–104](duplicate-expansion-recovery/) ·
[Recovery105–156/196–200](duplicate-expansion-recovery-105-156-and-196-200/).
''')
(out/'README.md').write_text(f'''# Complete200-seed disk-delay qualification

Executed reviewer verifies all16 archives/parts/every canonical member,
actual model executables/45 common repository dependency bytes against79915ca,
raw history operation counts/exact three-model outputs, exact1–200 coverage,
600s per seed/19 real dm-delay cuts/expected cohort audits/original30s p99 gates.
It binds original raw parser results to their authenticated inputs and API/job
metadata, without claiming a second invocation of every original raw parser.

Whole row:200 seeds/{r['invocations']:,} invocations/{r['journal_entries']:,} entries/
{r['faults']:,} actual faults/{r['model_operations']:,} exactOk operations/
{r['completed_cohort_audits']} cohort audits. Worst terminal/progress p99=
{r['worst_terminal_p99_seconds']:.9f}s/{r['worst_progress_p99_seconds']:.9f}s.
The recorded-source row qualifies. Workload SDK/full captured source/native stores
unavailable; final integrity/drain named-test scope. Final-source/full16×200/24h/
million physical drain and historical failed parent verdicts stay open/unchanged.
''')
for src,name in [('/tmp/js-wf-review-block-delay-full200-20261005.py','review-full200.py'),('/tmp/js-wf-block-delay-full200-20261005.log','executed-review.log'),(__file__,'publish-full200.py')]:shutil.copy2(src,out/name)
p=Path('/home/exedev/js-wf/docs/implementation-status.md');v=p.read_text().replace('disk-delay seeds 1–169 and 196–200 qualified','disk-delay seeds 1–200 qualified',1)
entry=f'''- **Complete recorded-source Tier3 real disk-delay200 row accepted:** final26
  seeds170–195 pass raw/device/admission/latency/explanation/fencing/cohort controls
  and three rebuilt models. All16 proof parts/canonical members verify; exact1–200
  coverage/600s/19 real cuts per seed/original30s gates and45 common model inputs
  match79915ca. Whole row621040 invocations/6840266 entries/3800 faults/798480 exactOk
  operations/2104 cohort audits; worst terminal/progress p99=5.954s/0.851s. This
  completes a second recorded-source Tier3 row. SDK/full captured workload source/
  native stores unavailable; integrity/drain named-test scope. Final-source/full
  matrices/24h/million-drain and historical failed parent verdicts stay open/unchanged.
  [Complete200 proof aggregation](scale/current-tier3-block-delay-2026-10-04/full200-qualification/).

''';v=v.replace('## Latest accepted evidence\n\n','## Latest accepted evidence\n\n'+entry,1);p.write_text(v)
p=Path('/home/exedev/js-wf/docs/implementation-plan.md');p.write_text(p.read_text()+'''\n### Complete recorded-source Tier3 real disk-delay200 row — 2026-10-05

All200 consecutive600s seeds at79915ca qualify. Full16-proof aggregation reads
all parts/canonical members and verifies actual model binaries/45 common Git
inputs, raw history counts/exact three-model outputs, exact seed coverage/19 real
faults per seed/expected cohort audits and original30s gates. Whole row621040
invocations/6840266 journal entries/3800 actual dm-delay faults/798480 exactOk
operations/2104 cohort audits; worst terminal/progress p99=5.954s/0.851s.
This completes two recorded-source Tier3 rows (ahead-clock and real disk delay).
Workload SDK/full captured source/native stores unavailable; final integrity/drain
named-test scope. Full/final-source matrices/24h/million-drain and historical
failed parent verdicts remain open/unchanged.
[Complete200 qualification](scale/current-tier3-block-delay-2026-10-04/full200-qualification/).
''')
print('PUBLISHED FULL200',r['invocations'],r['journal_entries'],r['faults'])
