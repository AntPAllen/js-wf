from pathlib import Path
import json,sys,subprocess,importlib.util,re,shutil,datetime
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-raft-safety170-synchronized-20261007');out=Path(str(root)+'-interrupted-proof');out.mkdir()
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
read=lambda p:json.loads(p.read_text())
before=read(root/'source-before.json');current=shared.source_inventory(before['revision']);assert current==before
assert fixture_archive.inventory(root/'fixture-source')==read(root/'fixture-source-before.json')
profiles={}
for label in ['upstream','contiguous']:
 live=read(root/(label+'-live-execution.json'));assert not Path('/proc',str(live['actual']['pid'])).exists()
 assert shared.sha(root/(label+'.test'))==live['actual']['actual_executable_sha256']
 log=(root/(label+'.log')).read_text();execution=root/(label+'-execution.json')
 profiles[label]=dict(started=re.findall(r'^=== RUN   (TestNRG\w+)$',log,re.M),passed=re.findall(r'^--- PASS: (TestNRG\w+) \(',log,re.M),failed=re.findall(r'^--- FAIL: (TestNRG\w+) \(',log,re.M),race_report='WARNING: DATA RACE' in log,native_exit_code=read(execution)['exit_code'] if execution.exists() else None,terminal_record_present=execution.exists(),complete_native_suite=execution.exists() and read(execution)['result']['qualified'])
assert profiles['upstream']['complete_native_suite'] and len(profiles['upstream']['passed'])==170
assert profiles['contiguous']['native_exit_code'] is None and not (root/'result.json').exists() and not (root/'source-after.json').exists()
for pid in [2594311,2594307]:assert not Path('/proc',str(pid)).exists()
closure=shared.closure(root)
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=before['revision'],recorded_before_and_current_source_equal=current,fixture_source_current_matches_before=True,profiles=profiles,producer_and_both_sdk_handles_missing=True,closure=closure,scope='Recovered observation after missing processes. Upstream completed170 exit0; contiguous has only partial raw output and no native exit/source-after/result. No candidate failure cause or full comparison qualification inferred. Original root unchanged; current source equality is an explicitly later observation, not an invented producer source-after.')
(out/'interruption-observation.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-preservation.py')
proof=fixture_archive.capture(root,Path(str(root)+'-interrupted-complete.tar.gz'),out,compresslevel=1)
print(json.dumps(dict(profiles={k:{f:(len(v) if isinstance(v,list) else v) for f,v in val.items()} for k,val in profiles.items()},archive=proof),indent=2))
