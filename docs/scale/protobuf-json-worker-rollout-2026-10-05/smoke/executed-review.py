from pathlib import Path
import json,hashlib,subprocess,shutil,tarfile
repo=Path('/home/exedev/js-wf');r=Path('/tmp/js-wf-protobuf-json-worker-smoke-20261005');out=Path('/tmp/js-wf-protobuf-json-worker-smoke-proof-20261005');out.mkdir()
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
e=json.loads((r/'execution.json').read_text());assert e['status']=='row_verified' and e['test_exit_code']==0 and e['duration']=='35s'
b=json.loads((r/'binary.json').read_text());assert sha(r/'integration.test')==b['sha256'] and not b['race'];assert 'vcs.modified=false' in b['build_info'] and e['source'] in b['build_info']
actual=json.loads(Path('/tmp/js-wf-protobuf-json-smoke-live-20261005/actual-sdks.json').read_text());assert len(actual['live_processes'])==6
for p in actual['live_processes']:assert p['sha256']==b['sha256'] and p['build_info'].splitlines()[1:]==b['build_info'].splitlines()[1:]
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text())
for n,d in before['files'].items():assert sha(r/'source'/n)==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d
manifest=json.loads((r/'archive-manifest.json').read_text());files=manifest['files'];seen=set()
with tarfile.open(r/'originals.tar.gz') as tar:
 for m in tar:
  assert m.isfile() and m.name in files and m.name not in seen
  assert hashlib.file_digest(tar.extractfile(m),'sha256').hexdigest()==files[m.name];seen.add(m.name)
assert seen==set(files)
q=json.loads((r/'result.json').read_text());assert q['shortened_smoke'] and q['invocations']==56 and q['journal_entries']==621 and q['confirmed_faults']==6
wire=q['journal_rollout_checks'];assert wire['mixed_invocations']==3 and wire['protobuf_worker_entries']==266 and wire['json_worker_entries']==355 and wire['all_retained_wire_bytes_independently_decoded']
for name in ['originals.tar.gz','archive-manifest.json','execution.json','result.json','binary.json','source-before.json','source-after.json','commands.json','events.jsonl','test-environment.json']:shutil.copyfile(r/name,out/name)
shutil.copyfile('/tmp/js-wf-protobuf-json-smoke-live-20261005/actual-sdks.json',out/'actual-sdks.json')
helper=out/'history-review.go';shutil.copyfile(r/'source/scripts/tier2-history-review.go.txt',helper)
model=r/'source';deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(helper)],cwd=model,text=True);names={'go.mod','go.sum'}
for line in deps.splitlines():
 d,g,c=line.split('|');directory=Path(d)
 if directory.is_relative_to(model):names.update(str((directory/n).relative_to(model)) for n in (g+' '+c).split())
hashes={n:sha(model/n) for n in names}
for n,d in hashes.items():
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==d
 p=out/'model-source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(model/n,p)
binary=out/'history-review';subprocess.run(['go','build','-p=1','-o',str(binary),str(helper)],cwd=model,check=True)
history=r/'fixture/history.jsonl';operations=sum(bool(l.strip()) for l in history.read_text().splitlines());p=subprocess.run([str(binary),str(history)],capture_output=True,text=True,check=True);assert p.stdout.splitlines()==[f'whole {n} operations={operations} verdict=Ok error=<nil>' for n in ['starts','signals','results']] and not p.stderr
(out/'model.stdout').write_text(p.stdout);(out/'model-dependencies.json').write_text(json.dumps(hashes,indent=2)+'\n');(out/'model-build-info.txt').write_text(subprocess.check_output(['go','version','-m',str(binary)],text=True))
review={'source':e['source'],'shortened_smoke_only':True,'named_pass_seconds':140.67,'invocations':56,'journal_entries':621,'confirmed_kills':6,'mixed_invocations':3,'actual_live_sdk_capture_scope':'Parent and five then-running worker children; not every killed/replacement generation independently observed','actual_sdk_sha256':b['sha256'],'all_live_captured_build_fields_match':True,'source_git_before_after_inputs':len(before['files']),'all_native_original_archive_members_verified':len(seen),'original_archive_sha256':sha(r/'originals.tar.gz'),'all_three_models_exact_ok':True,'model_operations':operations,'model_git_dependencies':len(hashes),'independent_wire_checks':wire,'native_server_scope':'Five-container named-test/retained binary/store scope; live per-container executable hashes not independently captured','physical_stores_scope':'Retained stopped stores, not independently reopened','qualifies_sustained_row':False,'qualifies_default_writer_profile':False,'qualifies_full_matrix':False,'qualifies_24h':False}
(out/'review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-review.py');print(json.dumps(review))
