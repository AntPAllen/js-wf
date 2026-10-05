from pathlib import Path
import json,hashlib,subprocess,shutil,tarfile,importlib.util,re,datetime
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-held-metadata-rollout-10m-20261005');out=Path('/tmp/js-wf-held-metadata-rollout-10m-proof-20261005');out.mkdir()
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
def ns(x):
 m=re.fullmatch(r'(.*?)(?:\.(\d+))?Z',x);d=datetime.datetime.fromisoformat(m[1]).replace(tzinfo=datetime.timezone.utc);return int(d.timestamp())*1000000000+int((m[2] or '').ljust(9,'0'))
e=json.loads((r/'execution.json').read_text());assert e['status']=='row_verified' and e['test_exit_code']==0 and e['duration']=='10m';b=json.loads((r/'binary.json').read_text());assert sha(r/'integration.test')==b['sha256'] and 'vcs.modified=false' in b['build_info']
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==e['source']
for n,d in before['files'].items():assert sha(r/'source'/n)==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d
manifest=json.loads((r/'archive-manifest.json').read_text());assert sha(r/'originals.tar.gz')==manifest['archive_sha256'];seen=set()
with tarfile.open(r/'originals.tar.gz') as tar:
 for m in tar:
  assert m.isfile() and m.name in manifest['files'] and m.name not in seen
  assert hashlib.file_digest(tar.extractfile(m),'sha256').hexdigest()==manifest['files'][m.name];seen.add(m.name)
assert seen==set(manifest['files'])
q=json.loads((r/'result.json').read_text());assert not q['shortened_smoke'] and q['invocations']==840 and q['journal_entries']==9268 and q['confirmed_faults']==119
assert all(x['terminal_p99_seconds']<30 and x['progress_p99_seconds']<30 for x in q['cells'].values());assert q['checkpoint_audit_checks']['completed_cohort_audits']==3
watch=Path('/tmp/js-wf-held-metadata-rollout-10m-live-20261005');sdks=[json.loads(x) for x in (watch/'sdks.jsonl').read_text().splitlines()];sessions=json.loads((r/'fixture/process-evidence.json').read_text());pids={s['pid'] for s in sessions};captured={s['pid'] for s in sdks};assert pids<=captured and len(pids)==124
for sdk in sdks:assert sdk['sha256']==b['sha256'] and sdk['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
assert json.loads((watch/'watch-result.json').read_text())['parent_gone']
for name in ['originals.tar.gz','archive-manifest.json','execution.json','result.json','binary.json','source-before.json','source-after.json','commands.json','events.jsonl','test-environment.json']:shutil.copy2(r/name,out/name)
shutil.copytree(watch,out/'actual-sdk-observer')
wireRoot=out/'copied-wire';wireRoot.mkdir()
for p in (r/'fixture').iterdir():
 if p.is_file() and (p.name.startswith('rollout-') or p.name.endswith('-encoding.json') or p.name in ['journal-rollout.json','process-evidence.json']):shutil.copy2(p,wireRoot/p.name)
spec=importlib.util.spec_from_file_location('wirechecker',repo/'scripts/check-journal-rollout.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
for path in ['scripts/check-journal-rollout.py','protocol/v1/journal.proto']:assert subprocess.check_output(['git','show',e['source']+':'+path],cwd=repo)==(repo/path).read_bytes()
wire=module.check(wireRoot,{'invocations':840});assert wire==q['journal_rollout_checks']
shutil.copy2(repo/'scripts/check-journal-rollout.py',out/'executed-wire-checker.py');shutil.copy2(repo/'protocol/v1/journal.proto',out/'journal.proto')
helper=out/'history-review.go';shutil.copy2(r/'source/scripts/tier2-history-review.go.txt',helper);model=r/'source';deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(helper)],cwd=model,text=True);names={'go.mod','go.sum'}
for line in deps.splitlines():
 d,g,c=line.split('|');directory=Path(d)
 if directory.is_relative_to(model):names.update(str((directory/n).relative_to(model)) for n in (g+' '+c).split())
hashes={n:sha(model/n) for n in names}
for n,d in hashes.items():
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d;p=out/'model-source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(model/n,p)
binary=out/'history-review';subprocess.run(['go','build','-p=1','-o',str(binary),str(helper)],cwd=model,check=True);history=r/'fixture/history.jsonl';operations=sum(bool(l.strip()) for l in history.read_text().splitlines());p=subprocess.run([str(binary),str(history)],capture_output=True,text=True,check=True);assert p.stdout.splitlines()==[f'whole {n} operations={operations} verdict=Ok error=<nil>' for n in ['starts','signals','results']] and not p.stderr
(out/'model.stdout').write_text(p.stdout);(out/'model-dependencies.json').write_text(json.dumps(hashes,indent=2)+'\n');(out/'model-build-info.txt').write_text(subprocess.check_output(['go','version','-m',str(binary)],text=True))
kills={f['worker']:f for f in json.loads((r/'fixture/faults.json').read_text())};held=[]
for path in sorted((r/'fixture').glob('*-dispatch.jsonl')):
 for line in path.read_text().splitlines():
  event=json.loads(line);o=event.get('HeldLease')
  if not o:continue
  assert event['Stage']=='lease_held' and o['key']==event['Type']+'.'+event['ID'];age=ns(o['observed_at'])-ns(o['created']) if o['entry_observed'] else None
  cut=kills.get(o.get('worker'));held.append({'file':path.name,'event':event,'client_observed_minus_server_created_ns':age,'owner_kill_before_observation':bool(cut and ns(cut['killed'])<ns(o['observed_at']))})
ages=[x for x in held if x['client_observed_minus_server_created_ns'] is not None];late=[x for x in ages if x['client_observed_minus_server_created_ns']>=12000000000]
(out/'held-entry-review.json').write_text(json.dumps({'all_records':held,'age_ge_12s':late,'clock_difference_not_expiry_authority':True,'cause_confirmed':False},indent=2)+'\n')
launch=json.loads((repo/'docs/scale/held-lease-observation-2026-10-05/10m-launch/review.json').read_text());assert launch['source']==e['source'] and launch['actual_sdk_sha256']==b['sha256'] and launch['actual_live_server_processes']==5
review={'source':e['source'],'named_pass_seconds':641.43,'invocations':840,'entries':9268,'kills':119,'cohort_audits':3,'cells':q['cells'],'independent_wire_checks':wire,'all_native_original_archive_members_verified':len(seen),'original_archive_sha256':sha(r/'originals.tar.gz'),'source_git_before_after_inputs':len(before['files']),'sdk_sha256':b['sha256'],'actual_worker_generation_pids_captured':len(pids),'worker_generations':124,'observer_records':len(sdks),'all_actual_sdk_hashes_and_build_fields_verified':True,'five_native_server_process_builds_referenced_to_immutable_launch':True,'all_three_rebuilt_models_exact_ok':True,'model_operations':operations,'model_git_dependencies':len(hashes),'held_metadata_records':len(held),'age_ge_12s':len(late),'maximum_client_minus_server_age_ns':max(x['client_observed_minus_server_created_ns'] for x in ages),'late_records_after_observed_owner_sigkill':sum(x['owner_kill_before_observation'] for x in late),'killed_process_final_counter_attribution_complete':False,'physical_stores_scope':'Complete stopped originals retained; named-test drain, no independent store reopen','shared_vm_overlap':'Million candidate throughout; copied-state-watch diagnostic started after this SDK was terminal','qualifies_sustained_opt_in_profile_seed1_only':True,'qualifies_default_writer_profile':False,'qualifies_full_matrix':False,'qualifies_24h':False,'historical_failed_parent_promoted':False,'causality_confirmed':False}
(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py');print(json.dumps(review))
