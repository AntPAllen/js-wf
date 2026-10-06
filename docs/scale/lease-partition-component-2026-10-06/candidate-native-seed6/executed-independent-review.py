from pathlib import Path
import sys,json,subprocess,hashlib,importlib.util,io,shutil,re,datetime
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
spec=importlib.util.spec_from_file_location('raw',repo/'scripts/check-tier2-journal-shard.py');raw=importlib.util.module_from_spec(spec);spec.loader.exec_module(raw)
r=Path('/tmp/js-wf-partition-seed6-candidate-20261006');outtmp=Path('/tmp/partition-candidate-native-review-20261006');outtmp.mkdir();e=json.loads((r/'execution.json').read_text());rev=e['source'];test='TestMixedMatrixServerPartitionEveryThirtySeconds'
assert rev==subprocess.check_output(['git','rev-parse','6a9bd09'],cwd=repo,text=True).strip() and e['status']=='passed' and e['exit_code']==0 and e['row']=='partition' and e['duration']=='10m' and e['race'] is False and e['server_observer_errors']==0 and e['server_profile']=='experimental-component-candidate'
assert not Path('/proc',str(e['pid'])).exists() and e['start_ticks'].isdigit()
command=json.loads((r/'commands.json').read_text());assert command['server_profile']==e['server_profile'] and command['test_command']==e['actual_argv']==[str(r/'integration.test'),'-test.run=^'+test+'$','-test.count=1','-test.v','-test.timeout=18m'] and command['build']==['go','test','-p=1','-buildvcs=true','-c','-o',str(r/'integration.test'),'./integration']
env=command['environment'];assert env==e['environment'] and (env['GOMAXPROCS'],env['GOMEMLIMIT'],env['WF_MATRIX_DURATION'],env['FAULT_SEED'])==('2','2GiB','10m','6') and env['WF_MATRIX_CHAOS']==env['WF_MATRIX_OPERATION_TIMINGS']=='1' and env['WF_MATRIX_PROCESS_ROOT']==str(r/'originals') and env['WF_MATRIX_PARTITION_SERVER_BIN']==str(r/'partition-server.bin') and 'WF_MATRIX_PARTITION_DIAGNOSTICS' not in env
binary=json.loads((r/'binary.json').read_text());assert binary['sha256']==e['sha256']==shared.sha(r/'integration.test') and 'vcs.revision='+rev in e['build_info'] and 'vcs.modified=false' in e['build_info'] and '-race=true' not in e['build_info']
assert subprocess.check_output(['go','version','-m',str(r/'integration.test')],text=True).splitlines()[1:]==e['build_info'].splitlines()[1:]
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[n for n in names if n.endswith('.go') or n in ('go.mod','go.sum')];assert set(expected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+n+'\n' for n in expected).encode()))
for n in expected:
 h=stream.readline().split();assert h[1]==b'blob';data=stream.read(int(h[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(data).hexdigest()==before['files'][n]==shared.sha(r/'source'/n)==shared.sha(repo/n)
assert not stream.read()
external=json.loads((r/'external-source-before.json').read_text());assert external==json.loads((r/'external-source-after.json').read_text());paths=json.loads((r/'external-captured-paths.json').read_text());assert set(paths)==set(external)
for path,digest in external.items():assert shared.sha(path)==shared.sha(r/paths[path])==digest
for row in json.loads((r/'generated-inputs.json').read_text()).values():assert shared.sha(r/row['captured'])==row['sha256']
for name,captured in [('run-tier2-retained-row.py','executed-producer.py'),('matrix_process_observer.py','matrix_process_observer.py'),('matrix_worker_observer.py','matrix_worker_observer.py'),('tier2_retained_profiles.py','tier2_retained_profiles.py'),('retained_input_cache.py','retained_input_cache.py'),('check-matrix-result.py','executed-checker.py')]:assert (r/captured).read_bytes()==subprocess.check_output(['git','cat-file','blob',rev+':scripts/'+name],cwd=repo)
parent=json.loads((r/'partition-server-input.json').read_text());assert parent['captured']=='partition-server.bin' and parent['parent_proof_revision']==rev and shared.sha(r/parent['captured'])==parent['sha256']
canonical=Path(parent['parent_proof']);proofdir=r/'inputs/partition-server-proof'
for name,digest in parent['retained_parent_records'].items():
 data=subprocess.check_output(['git','cat-file','blob',rev+':'+str(canonical/name)],cwd=repo);assert hashlib.sha256(data).hexdigest()==digest==shared.sha(proofdir/name) and data==(repo/canonical/name).read_bytes()
parent_review=json.loads((proofdir/'independent-review.json').read_text());parent_build=json.loads((proofdir/'candidate-build.json').read_text());parent_meta=json.loads((proofdir/'archive-verification.json').read_text());parent_receipt=json.loads((proofdir/'s3-readback.json').read_text())
assert parent_review['candidate_sha256']==parent_build['executable_sha256']==parent['sha256'] and parent_review['result']['recovered'] is True and parent_review['source']==parent_build['source']==parent['parent_source'] and parent_meta['archive_sha256']==parent['parent_archive_sha256'] and parent_receipt['archive']['url']==parent['parent_archive_url'] and parent_receipt['archive']['full_readback']=={'bytes':parent_meta['archive_bytes'],'sha256':parent_meta['archive_sha256']}
verification=json.loads((r/'partition-server-verification.json').read_text());assert verification['passed'] is True and verification['expected_sha256']==parent['sha256'] and verification['observed_peers']==3
servers=json.loads((r/'observed-servers.json').read_text());assert len(servers)==3 and sorted(s['node'] for s in servers)==[0,1,2] and len({(s['pid'],s['start_ticks']) for s in servers})==3
for server in servers:
 assert not Path('/proc',str(server['pid'])).exists() and server['start_ticks'].isdigit() and shared.sha(r/server['captured'])==server['actual_executable_sha256']==parent['sha256']
 argv=server['argv'];assert argv[0]==str(r/'partition-server.bin') and '-D' not in argv and argv[argv.index('-sd')+1]==server['store']==str(r/'originals'/test/('node-'+str(server['node']))) and argv[argv.index('-n')+1]==f"wf-process-{server['node']}"
 assert server['build_info'].splitlines()[1:]==parent['build_info'].splitlines()[1:] and re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s',server['build_info'])
assert json.loads((r/'observed-workers.json').read_text())==[]
acceptance=json.loads((r/'acceptance.json').read_text());assert acceptance['exit_code']==acceptance['native_exit_code']==0 and acceptance['server_profile']==e['server_profile'] and acceptance['source']==rev and acceptance['duration']=='10m'
initial=fixture_archive.inventory(r);closure=shared.closure(r)
segment=(r/'native.log').read_text()+'\n'+(r/'acceptance.log').read_text()
raw_review=raw.review_raw(rev,r,6,6,{6:segment},model_root=repo,model_binary_out=outtmp/'history-model',row='partition',event_files={6:'converted-events.jsonl'})
assert fixture_archive.inventory(r)==initial
faults=json.loads((r/'matrix-partition-6-faults.json').read_text())['faults'];recoveries=[(raw.timestamp_ns(f['healed'])-raw.timestamp_ns(f['killed']))/1e9 for f in faults];assert len(recoveries)==19 and max(recoveries)<35
archive=Path('/tmp/js-wf-partition-seed6-candidate-complete-20261006.tar.gz');archiveproof=outtmp/'complete-proof';meta=fixture_archive.capture(r,archive,archiveproof,compresslevel=1);assert fixture_archive.inventory(r)==initial
out=repo/'docs/scale/lease-partition-component-2026-10-06/candidate-native-seed6';out.mkdir()
for name in ['execution.json','commands.json','acceptance.json','acceptance.log','binary.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','external-captured-paths.json','generated-inputs.json','observed-servers.json','observed-workers.json','partition-server-input.json','partition-server-verification.json','native.log','matrix-partition-6-faults.json','matrix-partition-6-latencies.json']:shutil.copyfile(r/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(archiveproof/name,out/name)
(out/'independent-raw-review.json').write_text(json.dumps(raw_review,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-independent-review.py')
report=dict(source=rev,parent_server_source=parent['parent_source'],candidate_sha256=parent['sha256'],selected_Git_sources=len(expected),external_inputs=len(external),source_Git_current_retained_before_after_equal=True,actual_SDK_executable_argv_environment_birth_verified=True,three_candidate_server_executable_argv_birth_and_source_proof_bindings_verified=True,server_profile=e['server_profile'],faults=19,recovery_cut_seconds=recoveries,max_recovery_cut_seconds=max(recoveries),raw_data_verified=raw_review['raw_data_verified'],history_models=3,invocations=raw_review['invocations'],journal_entries=raw_review['journal_entries'],archive=meta,closure=closure,original_fixture_unchanged_during_independent_review=True,qualifies_default_production_server=False,qualifies_original_failed_campaign=False,qualifies_full_matrix=False,qualifies_workflow_Tier1=False,scope='One sustained normal10m SDK partition seed6 with exact source-bound experimental component candidate; original35s replica-current gate, raw19faults, latency corpus, three independent history models and named native final integrity/drain assertions. No broker/store opened in review; default dependency, failed original200 and full qualification gates unchanged.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ('archive','closure')},indent=2))
