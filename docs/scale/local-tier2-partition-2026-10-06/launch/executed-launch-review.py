from pathlib import Path
import hashlib,json,subprocess,os,datetime
repo=Path('/home/exedev/js-wf');checkout=Path('/tmp/js-wf-local-partition200-source-20261006');root=Path('/tmp/js-wf-local-partition200-20261006');seed=root/'seed-001'
def sha(p):
 with Path(p).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()
campaign=json.loads((root/'campaign.json').read_text());execution=json.loads((seed/'execution.json').read_text());commands=json.loads((seed/'commands.json').read_text());prep=json.loads((root/'prepared/preparation.json').read_text())
source=campaign['source'];assert campaign['seeds']==list(range(1,201)) and campaign['duration']=='10m' and not campaign['race']
assert campaign['status']=='running' and campaign['current_seed']==1 and not campaign['qualifies_full_row']
assert execution['source']==source and execution['status']=='running' and execution['duration']=='10m' and not execution['race']
pid=execution['pid'];proc=Path('/proc')/str(pid);birth=proc.joinpath('stat').read_text().rsplit(')',1)[1].split()[19];argv=proc.joinpath('cmdline').read_bytes().decode().rstrip('\0').split('\0')
assert birth==execution['start_ticks'] and argv==execution['actual_argv']==commands['test_command']
assert argv[1:]==['-test.run=^TestMixedMatrixServerPartitionEveryThirtySeconds$','-test.count=1','-test.v','-test.timeout=18m']
assert sha(proc/'exe')==execution['sha256']==sha(seed/'integration.test')==prep['sha256']==sha(root/'prepared/integration.test')
assert 'vcs.revision='+source in execution['build_info'] and 'vcs.modified=false' in execution['build_info'] and '-race=true' not in execution['build_info']
profile={}
for value in proc.joinpath('environ').read_bytes().split(b'\0'):
 key,sep,val=value.partition(b'=')
 if key in (b'GOMAXPROCS',b'GOMEMLIMIT',b'FAULT_SEED',b'WF_MATRIX_DURATION',b'WF_MATRIX_CHAOS') and sep:profile[key.decode()]=val.decode()
assert profile==dict(GOMAXPROCS='2',GOMEMLIMIT='2GiB',FAULT_SEED='1',WF_MATRIX_DURATION='10m',WF_MATRIX_CHAOS='1')
selected=json.loads((seed/'source-before.json').read_text());assert selected==json.loads((root/'prepared/source-before.json').read_text())==json.loads((root/'prepared/source-after.json').read_text())
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=checkout,text=True).splitlines();assert set(names)==set(selected['files'])
for name,digest in selected['files'].items():
 assert sha(seed/'source'/name)==sha(root/'prepared/source'/name)==sha(checkout/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+name],cwd=repo)).hexdigest()==digest
external=json.loads((seed/'external-source-before.json').read_text());assert external==json.loads((root/'prepared/external-source-before.json').read_text())==json.loads((root/'prepared/external-source-after.json').read_text());paths=json.loads((seed/'external-captured-paths.json').read_text());assert set(paths)==set(external)
for name,digest in external.items():assert sha(seed/paths[name])==sha(name)==digest
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
unit=subprocess.check_output(['systemctl','--user','show','js-wf-local-partition200-20261006.service','-p','MainPID','-p','ActiveState','-p','MemoryMax','-p','CPUQuotaPerSecUSec','-p','Restart','-p','KillMode','-p','RuntimeMaxUSec'],text=True)
assert 'MainPID='+str(campaign['producer_pid']) in unit and 'ActiveState=active' in unit and 'MemoryMax=4294967296' in unit and 'CPUQuotaPerSecUSec=2s' in unit and 'Restart=no' in unit and 'KillMode=control-group' in unit
assert birth==proc.joinpath('stat').read_text().rsplit(')',1)[1].split()[19]
report=dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source=source,campaign_root=str(root),source_checkout=str(checkout),unit=unit,actual_sdk_pid=pid,actual_sdk_start_ticks=birth,actual_sdk_sha256=execution['sha256'],actual_sdk_argv=argv,actual_profile=profile,selected_git_inputs_verified=len(names),selected_external_inputs_verified=len(external),original_seeds=list(range(1,201)),native_coverage_complete=False,qualifies_full_row=False,original24h_sdk_alive=Path('/proc/3461745').exists(),scope='Live launch binding only; no terminal native, 200-seed, source-after, physical-store or full-matrix acceptance.')
out=repo/'docs/scale/local-tier2-partition-2026-10-06/launch';out.mkdir(exist_ok=False)
(out/'launch-review.json').write_text(json.dumps(report,indent=2)+'\n');(out/'executed-launch-review.py').write_bytes(Path(__file__).read_bytes())
for name in ('campaign.json','prepared/preparation.json','seed-001/execution.json','seed-001/commands.json'):
 target=out/Path(name).name
 if target.exists():target=out/('seed-001-'+Path(name).name)
 target.write_bytes((root/name).read_bytes())
print(json.dumps(report,indent=2))
