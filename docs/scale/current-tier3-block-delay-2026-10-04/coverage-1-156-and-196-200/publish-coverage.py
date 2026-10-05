from pathlib import Path
import json,shutil
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/current-tier3-block-delay-2026-10-04';out=base/'coverage-1-156-and-196-200'
r=json.loads((out/'coverage-qualification.json').read_text());assert r['accepted_seeds']==161 and r['all_archive_members_and_parts_verified'] and r['all_actual_models_and_dependencies_verified']
rows=r['shards'];table=[]
for s in rows:
 first,last=s['first'],s['last'];d=base/f'seeds-{first}-{last}';m=json.loads((d/'manifest.json').read_text())
 table.append(f"| [{first}–{last}](seeds-{first}-{last}/) | {s['job']} | {s['invocations']:,} | {s['journal_entries']:,} | {s['faults']:,} | {s['model_operations']:,} |")
 if first<105:continue
 (d/'README.md').write_text(f"""# Real disk-delay seeds {first}–{last}: accepted at recorded source

Run37164231641/job{s['job']}/artifact{s['artifact']} binds this completed
600-second-per-seed shard to79915ca41a5c5a23b9997eea5f3f66d82530ee30.
All{last-first+1} seeds independently qualify{s['invocations']:,} invocations,
{s['journal_entries']:,} entries,{s['faults']} confirmed faults and
{s['model_operations']:,} exactOk model operations. Real writable node4 stores,
R5 leader admission,100ms dm-delay/minimum5s intervals/delayed sync and restoration
verify. All{s['completed_cohort_audits']} expected completed-cohort audits pass.
Worst terminal/progress p99={s['worst_terminal_p99_seconds']:.9f}s/
{s['worst_progress_p99_seconds']:.9f}s under unchanged30s gates. Each model's
45 actual local Go/module inputs independently matches Git.

Raw authenticated ZIP/expansion, metadata/job logs/transfer attempts, reviewers,
model executable/build information/source and exact outputs are preserved.
Every{len(m['files'])} canonical member and{len(m['parts'])} published part reads
back. Compressed size{m['archive_bytes']:,} bytes; SHA256
`{m['archive_sha256']}`. Concatenate sorted numbered parts and verify the manifest
before extracting fresh. The combined coverage reviewer also verifies all members,
actual model/source identities, raw-history operation counts and exact outputs.

Workload attribution is checkout/header binding; full captured workload source,
actual SDK and native stores are unavailable. Final integrity/drain retains
named-test assertion scope; no physical reopening or causality claim. These seeds
qualify only their recorded source. Full200/final-source/full16×200/24h/million
physical-drain gates and historical failed originals remain open/unchanged.
""")
aggregate={k:v for k,v in r.items() if k!='model_dependency_sha256'}
(base/'aggregate.json').write_text(json.dumps(aggregate,indent=2)+'\n')
(base/'README.md').write_text(f"""# Recorded-source Tier3 disk-delay qualification

Thirteen completed successful shards bind to79915ca in run37164231641.
Seeds1–156 and196–200 qualify;157–195 are not yet accepted. All161 accepted
seeds pass independently regenerated raw fault/admission/report/explanation/
fencing checks and all three rebuilt history models.

| Seeds | Job | Invocations | Entries | Faults | Model operations |
| --- | --- | ---: | ---: | ---: | ---: |
"""+'\n'.join(table)+f"""
| Total | | {r['invocations']:,} | {r['journal_entries']:,} | {r['faults']:,} | {r['model_operations']:,} |

All{r['completed_cohort_audits']} expected completed-cohort audits verify. Worst
terminal/progress p99={r['worst_terminal_p99_seconds']:.9f}s/
{r['worst_progress_p99_seconds']:.9f}s under original30s gates. Each actual model's
45 local dependencies matches executed Git. The combined reviewer reads every
part/canonical member in all13 proofs, verifies model executables/source inputs,
raw history operation counts, exact three-model outputs and600s/30s gates.
[Combined executed review](coverage-1-156-and-196-200/).

Workload source attribution is checkout/header binding. Full captured workload
source inventories, actual SDK and native stores are unavailable; integrity/drain
retain named-test scope. No physical reopening or server cause claim. Full200/
final-source/full16×200/24h and million-drain gates remain open; failed originals
remain preserved and the parent has other failed rows.

Older accepted raw expansions1–104 were removed only after complete ZIP/canonical/
published-input verification. Retained ZIPs/proofs restore every byte.
[Exact executed recovery](duplicate-expansion-recovery/).
""")
(out/'README.md').write_text(f"""# Combined disk-delay coverage review

The executed reviewer verifies all13 archives/parts/every canonical member,
all actual model executables,45 common repository dependency bytes against
79915ca, every raw history operation count and exact three-model output, exact
seed coverage,600s per-seed duration,19 confirmed real dm-delay cuts per seed,
expected completed-cohort audits and unchanged30s p99 gates. It verifies each
recorded row's raw-input and API/job-log hashes against canonical member ledgers.
The original per-shard raw fault/admission/explanation/fencing reviews are retained;
this aggregation binds their inputs and results, rather than independently
repeating the full original raw parser a second time.

Accepted continuous1–156 plus196–200:161 distinct seeds /{r['invocations']:,}
invocations /{r['journal_entries']:,} journal entries /{r['faults']:,} faults /
{r['model_operations']:,} exactOk model operations /{r['completed_cohort_audits']}
cohort audits. Worst terminal/progress p99={r['worst_terminal_p99_seconds']:.9f}s/
{r['worst_progress_p99_seconds']:.9f}s. Missing157–195 remain open. Workload SDK/full
captured source/native stores are unavailable; integrity/drain is named-test scope.
No full row, final-source matrix,24h or physical-drain qualification.
""")
for src,name in [('/tmp/js-wf-review-block-delay-coverage-20261005.py','review-coverage.py'),('/tmp/js-wf-block-delay-coverage-20261005.log','executed-review.log'),(__file__,'publish-coverage.py')]:shutil.copyfile(src,out/name)
p=repo/'docs/implementation-status.md';s=p.read_text();s=s.replace('disk-delay seeds 1–104 qualified at executed `79915ca`','disk-delay seeds 1–156 and 196–200 qualified at executed `79915ca`',1)
entry=f"""- **Tier3 disk-delay coverage extends to161 accepted seeds:** five additional
  successful shards independently qualify57 seeds /175560 invocations /1933432
  entries /1083 real dm-delay faults /225720 exactOk model operations. Combined
  review reads every member/part in all13 proofs and checks actual model binaries,
  45 common Git dependencies, raw history counts/exact outputs and original600s/
  30s gates. Continuous1–156 plus196–200 totals{r['invocations']} invocations /
  {r['journal_entries']} entries /{r['faults']} faults /{r['model_operations']} operations /
  {r['completed_cohort_audits']} cohort audits; worstp99={r['worst_terminal_p99_seconds']:.3f}s/
  {r['worst_progress_p99_seconds']:.3f}s. Workload source checkout/header binding;
  SDK/full captured source/stores unavailable, integrity/drain named-test scope.
  Missing157–195/full200/final-source/full matrices/24h/million-drain stay open.
  [Complete13-proof coverage review](scale/current-tier3-block-delay-2026-10-04/coverage-1-156-and-196-200/).

"""
s=s.replace('## Latest accepted evidence\n\n','## Latest accepted evidence\n\n'+entry,1);p.write_text(s)
p=repo/'docs/implementation-plan.md';s=p.read_text()+f"""\n### Recorded-source disk-delay161-seed coverage — 2026-10-05

Five newly completed shards at79915ca extend accepted continuous coverage to1–156
and separately196–200. Full13-proof aggregation verifies all parts/canonical
members, actual model binaries/45 common Git dependency inputs, raw history
operation counts/exact three-model outputs and600s/30s original gates. All161
accepted seeds total{r['invocations']:,} invocations/{r['journal_entries']:,} entries/
{r['faults']:,} real faults/{r['model_operations']:,} model operations/
{r['completed_cohort_audits']} cohort audits. Worst terminal/progress p99=
{r['worst_terminal_p99_seconds']:.3f}s/{r['worst_progress_p99_seconds']:.3f}s.
Workload SDK/full captured source/native stores unavailable; final integrity/drain
named-test scope. Missing157–195/full200/final-source/full16×200/24h/million drain
remain open; original failed parents are not promoted.
[Complete coverage review](scale/current-tier3-block-delay-2026-10-04/coverage-1-156-and-196-200/).
""";p.write_text(s)
print('PUBLISHED',r['accepted_seeds'],r['invocations'],r['journal_entries'],r['completed_cohort_audits'])
