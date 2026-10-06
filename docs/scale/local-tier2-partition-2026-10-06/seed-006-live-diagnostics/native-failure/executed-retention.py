from pathlib import Path
import json,subprocess,sys,hashlib,shutil,importlib.util
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
r=Path('/tmp/js-wf-partition-seed6-live-diagnostics-20261006');out=repo/'docs/scale/local-tier2-partition-2026-10-06/seed-006-live-diagnostics/native-failure'
e=json.loads((r/'execution.json').read_text());rev=e['source'];assert e['exit_code']==1 and e['status']=='failed' and e['environment']['WF_MATRIX_PARTITION_DIAGNOSTICS']=='1'
assert e['actual_argv'][-1]=='-test.timeout=18m' and e['duration']=='10m' and not e['race'] and e['server_observer_errors']==0
assert not Path('/proc',str(e['pid'])).exists()
before=json.loads((r/'source-before.json').read_text());assert before==json.loads((r/'source-after.json').read_text()) and before['revision']==rev
for name,digest in before['files'].items():assert shared.sha(r/'source'/name)==digest==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',rev+':'+name],cwd=repo)).hexdigest()
assert json.loads((r/'external-source-before.json').read_text())==json.loads((r/'external-source-after.json').read_text())
servers=json.loads((r/'observed-servers.json').read_text());assert len(servers)==3 and len({s['pid'] for s in servers})==3
assert all(shared.sha(r/s['captured'])==s['actual_executable_sha256'] and not Path('/proc',str(s['pid'])).exists() for s in servers)
checker=subprocess.check_output(['git','cat-file','blob',rev+':scripts/check-matrix-result.py'],cwd=repo);(r/'executed-checker.py').write_bytes(checker)
convert=['go','tool','test2json','-t','-p','js-wf/integration'];check=['python3',str(r/'executed-checker.py'),str(r/'converted-events.jsonl'),e['test'],'10m']
with (r/'native.log').open('rb') as source,(r/'converted-events.jsonl').open('wb') as output:subprocess.run(convert,stdin=source,stdout=output,check=True)
with (r/'acceptance.log').open('w') as output:code=subprocess.run(check,stdout=output,stderr=subprocess.STDOUT).returncode
assert code!=0
(r/'acceptance.json').write_text(json.dumps(dict(exit_code=code,native_exit_code=1,duration='10m',source=rev,row='partition',scope='Independent post-run finalization after wrapper duplicated late flag access; original native failure unchanged.'),indent=2)+'\n')
(r/'independent-finalization.json').write_text(json.dumps(dict(convert=convert,check=check,checker_sha256=hashlib.sha256(checker).hexdigest(),producer_failed_after_native_and_source_ledgers=True,producer_error="AttributeError: list has no attribute partition_diagnostics",native_rerun=False),indent=2)+'\n')
files=list(r.glob('*peers.jsonl'));assert len(files)==1
rounds=[json.loads(l) for l in files[0].read_text().splitlines()];assert rounds and [v['round'] for v in rounds]==list(range(1,len(rounds)+1))
rows=[];ids={n:set() for n in range(3)}
for round in rounds:
 assert [p['node'] for p in round['peers']]==[0,1,2]
 for peer in round['peers']:
  if 'jetstream' not in peer:continue
  public=peer['jetstream'];ids[peer['node']].add(public['server_id'])
  lease=[s for a in public.get('account_details',[]) if a['name']=='$G' for s in a.get('stream_detail',[]) if s['name']=='KV_WF_LEASE'];assert len(lease)==1
  rows.append(dict(round=round['round'],scheduled=round['scheduled'],node=peer['node'],observed_at=peer['observed_at'],routes=peer['routes'],cluster=lease[0]['cluster'],state=lease[0]['state']))
assert all(len(v)==1 for v in ids.values()) and len(set().union(*ids.values()))==3
closure=shared.closure(r)
proof=fixture_archive.capture(r,r.with_suffix('.tar.gz'),out,compresslevel=1)
for name in ['execution.json','commands.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','observed-servers.json','acceptance.json','independent-finalization.json','native.log','acceptance.log']:shutil.copyfile(r/name,out/name)
for file in files:shutil.copyfile(file,out/file.name)
shutil.copyfile(next(r.glob('*-faults.json')),out/'faults.json')
(out/'lease-peer-observations.json').write_text(json.dumps(rows,indent=2)+'\n')
(out/'independent-retention.json').write_text(json.dumps(dict(source=rev,original_native_failure_unchanged=True,source_Git_retained_before_after_equal=True,actual_sdk_profile_verified=True,observed_servers=3,recorded_live_rounds=len(rounds),local_peer_ids={n:next(iter(v)) for n,v in ids.items()},closure=closure,complete_archive=proof,qualifies_full_row=False,scope='Instrumented original seed6 reproduces first replica-heal timeout with observed KV_WF_LEASE stall; the final diagnostic error names WF_INV after its request deadline. Local peer stream progress is retained. Post-native producer flag access separately repaired; no timeout relaxed, no native rerun, no confirmed server cause or causal Tier1 reproduction.'),indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-retention.py');print(json.dumps(proof))
