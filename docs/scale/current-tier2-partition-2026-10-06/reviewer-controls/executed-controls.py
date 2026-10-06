from pathlib import Path
import sys,json,shutil,importlib.util,hashlib,datetime,zipfile
repo=Path('/home/exedev/js-wf');base=Path('/tmp/js-wf-partition-review-controls-20261006');base.mkdir()
spec=importlib.util.spec_from_file_location('reviewer',repo/'scripts/check-tier2-journal-shard.py');reviewer=importlib.util.module_from_spec(spec);spec.loader.exec_module(reviewer)
original=Path('/tmp/js-wf-partition-review-positive-artifacts-20261006');before=reviewer.shared.inventory(original)
zip_path=Path('/tmp/js-wf-partition-review-positive-provider-20261006.zip')
with zipfile.ZipFile(zip_path) as archive:
 actual={}
 for member in archive.infolist():
  assert not member.is_dir() and member.filename not in actual
  actual[member.filename]=hashlib.sha256(archive.read(member)).hexdigest()
 assert actual==before
run=json.loads(Path('/tmp/js-wf-partition-review-positive-run-20261006.json').read_text());job=json.loads(Path('/tmp/js-wf-partition-review-positive-job-20261006.json').read_text());artifact=json.loads(Path('/tmp/js-wf-partition-review-positive-artifact-20261006.json').read_text());log=Path('/tmp/js-wf-partition-review-positive-log-20261006.txt').read_text()
assert artifact['digest']=='sha256:'+hashlib.sha256(zip_path.read_bytes()).hexdigest()
records={}
for mode in ('majority_write_missing','latency_mismatch','second_successful_start'):
 root=base/mode;shutil.copytree(original,root)
 if mode=='majority_write_missing':
  path=root/'matrix-partition-1-faults.json';value=json.loads(path.read_text());value['faults'][0]['majority_sequence']=0;path.write_text(json.dumps(value))
 elif mode=='latency_mismatch':
  path=root/'matrix-partition-1-latencies.json';value=json.loads(path.read_text());value[0]['delay_ns']+=1;path.write_text(json.dumps(value))
 else:
  path=root/'matrix-partition-1-history.jsonl';values=[json.loads(l) for l in path.read_text().splitlines()];duplicate=dict(values[0]);duplicate['invoke_ts']=values[0]['return_ts'];duplicate['return_ts']=values[0]['return_ts'];values.append(duplicate);path.write_text(''.join(json.dumps(v)+'\n' for v in values))
 try:
  reviewer.review(run,job,artifact,log,root,1,1,model_root='/tmp/js-wf-partition-review-positive-source-20261006',row='partition')
 except ValueError as exc:records[mode]=dict(rejected=True,error=str(exc))
 else:raise AssertionError('mutated artifact qualified: '+mode)
 assert reviewer.shared.inventory(original)==before
 print('REAL_DATA_NEGATIVE_REJECTED',mode,records[mode]['error'],flush=True)
report=dict(provider_artifact_digest_verified=artifact['digest'],extracted_members_equal_provider_zip=len(before),original_bytes_unchanged=True,controls=records,reviewer_sha256=hashlib.sha256((repo/'scripts/check-tier2-journal-shard.py').read_bytes()).hexdigest(),scope='Fresh provider-copy positive control and three independently mutated disposable copies; no NATS or original stores opened. Historical executed-source control only; no current200/fullmatrix qualification.')
(base/'controls.json').write_text(json.dumps(report,indent=2)+'\n')
(base/'executed-controls.py').write_bytes(Path(__file__).read_bytes())
