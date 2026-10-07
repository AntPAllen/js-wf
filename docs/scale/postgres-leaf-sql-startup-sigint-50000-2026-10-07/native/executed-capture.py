import sys,json,subprocess,shutil,datetime
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-postgres-leaf-sql-startup-sigint-50000-v2-20261007');proof=root.with_name(root.name+'-proof');out=repo/'docs/scale/postgres-leaf-sql-startup-sigint-50000-2026-10-07/native';out.mkdir()
review=json.loads((proof/'independent-review.json').read_text());assert review['source']=='989330dd9455e3122cc64c56691b2daad70bba12' and review['rows']
assert review['sql_startup_cancellation']['signal']=='SIGINT' and review['execution']['status']=='passed'
startup=json.loads((proof/'startup-controls.json').read_text());inherited=json.loads((proof/'coverage-controls.json').read_text());assert startup['rejected_mutations']==39 and inherited['rejected_mutations']==32
for p in proof.iterdir():
 if p.is_file():shutil.copyfile(p,out/p.name)
for n in ['source-before.json','source-after.json','external-source-before.json','external-source-after.json','external-captured-paths.json','actual-sdk.json','actual-postgres.json','postgres-stopped.json','postgres-image.json','postgres-media-copy-verification.json','commands.json','binary.json','projector-profile.json','execution.json','closure.json','native.log','build.log','postgres-before-stop.log']:shutil.copyfile(root/n,out/n)
case=next((root/'originals').rglob('projection-fault-proof.json')).parent
for n in ['projection-fault-proof.json','sql-startup-boundary-progress.json','projection-dependency-trace.json']:shutil.copyfile(case/n,out/n)
for name in ['review-postgres-domain-projection.py','sql_startup_cancellation.py','check-postgres-sql-startup-proof-controls.py','check-postgres-standalone-proof-controls.py','sql_leaf_stream_wire.py']:shutil.copyfile(repo/'scripts'/name,out/('executed-'+name))
shutil.copyfile(__file__,out/'executed-capture.py')
(out/'review-provenance.json').write_text(json.dumps({'executed_native_source':review['source'],'verifier_source':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'verifier_only_followup':'The report scope now names the exact selected startup signal instead of hardcoded SIGTERM. No native rerun after that scope correction. Original selected runner/validator/reviewer bytes remain in full archive.','captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
print(json.dumps({k:review[k] for k in ['selected_git_inputs','selected_external_inputs','closed_sql_media_files','execution']}))
