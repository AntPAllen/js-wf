import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,subprocess,importlib.util,io,re,shutil,copy,tempfile
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive,worker_leaf_wire
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-worker-domain-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
root=Path('/tmp/js-wf-worker-leaf-wire-20261007');meta_root=root.with_name(root.name+'-proof');out=Path('/tmp/js-wf-worker-leaf-wire-independent-20261007')
read=lambda p:json.loads(p.read_text())
unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show',root.name+'.service','-p','MainPID','-p','SubState','-p','ExecMainStatus'],text=True).splitlines())
assert unit==dict(MainPID='0',SubState='failed',ExecMainStatus='1')
execution=read(root/'execution.json');rev=execution['source'];assert execution['exit_code']==0
before=read(root/'source-before.json');assert before==read(root/'source-after.json') and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[name for name in names if name.endswith(('.go','.py','.yml')) or name in ('go.mod','go.sum') or name.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
blobs=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+name+'\n' for name in expected).encode()))
for name in expected:
 header=blobs.readline().split();assert header[1]==b'blob';body=blobs.read(int(header[2]));assert blobs.read(1)==b'\n';assert hashlib.sha256(body).hexdigest()==before['files'][name]==shared.sha(root/'selected-source'/name)
assert not blobs.read()
sdk=read(root/'actual-sdk.json');binary=read(root/'binary.json');commands=read(root/'commands.json')
assert sdk['args']==commands['run'] and sdk['args'][2]=='-test.run=^('+worker_leaf_wire.TEST+')$' and sdk['args'][-2:]==['-test.count=1','-test.timeout=4m']
assert sdk['working_directory']==commands['run_working_directory']==str(repo/'cmd/wf-worker') and commands['build_working_directory']==str(repo)
assert sdk['exe_sha256']==binary['sha256']==shared.sha(root/'worker-race.test')
assert '-race=true' in binary['build_info'] and 'vcs.revision='+rev in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
assert sdk['admission']['stable_identity_observed_twice'] and not Path('/proc',str(sdk['pid'])).exists()
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='',WF_WORKER_TEST_ROOT=str(root/'stores'),WF_WORKER_STANDALONE='1')
plugins=read(root/'plugins.json');assert len(plugins)==1 and '-race=true' in plugins[0]['build_info'] and shared.sha(plugins[0]['path'])==plugins[0]['sha256']
children=read(root/'standalone-processes.json');assert len(children)==len({child['pid'] for child in children})==3 and len({child['exe_sha256'] for child in children})==1
records=list((root/'stores').rglob('standalone.process.json'));assert len(records)==3
for path in records:
 child=read(path);assert child in children
 for name in ('stdout','stderr'):assert shared.sha(path.parent/name)==child[name+'_sha256']
 for field,value in (('exit_code',0),('signal','SIGTERM'),('domain','WFWORKER')):assert child[field]==value
 assert not Path('/proc',str(child['pid'])).exists() and child['stat'].split()[0]==str(child['pid']) and child['stat'].rsplit(')',1)[1].split()[19].isdigit()
 args=child['argv'];assert args[0]==child['exe'] and Path(child['exe']).is_relative_to(root/'stores')
 assert shared.sha(child['exe'])==child['exe_sha256'] and subprocess.check_output(['go','version','-m',child['exe']],text=True)==child['build_info']
 assert 'vcs.revision='+rev in child['build_info'] and 'vcs.modified=false' in child['build_info'] and '-race=true' in child['build_info']
 assert args[args.index('-handler-plugin')+1]==plugins[0]['path'] and args[args.index('-replicas')+1]=='3' and args[args.index('-domain')+1]=='WFWORKER'
 mode=child['test'].split('/')[-1];assert args[args.index('-mode')+1]==mode
 if mode=='static':assert args[args.index('-journal-max-bytes')+1]=='131072'
 else:assert args[args.index('-timer-backend')+1]=='native'
log=(root/'native.log').read_text();qualification=native.verify_leaf_log(log)
assert not (root/'row-review.json').exists()
assert 'TypeError: unhashable type:' in (meta_root/'original-collector-failure.log').read_text()
logged=re.findall(r'worker standalone process domain="WFWORKER" pid=(\d+) signal=SIGTERM exit=0 exe_sha256=([0-9a-f]{64})',log)
assert {(int(pid),digest) for pid,digest in logged}=={(child['pid'],child['exe_sha256']) for child in children}
reports=[];leaves=[];hub_ids=set()
for proof_path in (root/'stores').glob('wf-worker-leaf-*/leaf-proof.json'):
 proof=read(proof_path);report=worker_leaf_wire.validate(proof_path.parent);reports.append(report)
 child=next(child for child in children if child['test']==proof['test']);args=child['argv'];assert args[args.index('-url')+1]==proof['proxy_url']
 leaf=read(proof_path.parent/'leaf.process.json');leaves.append(leaf)
 assert leaf['pid']==proof['leaf_pid'] and leaf['reaped'] and leaf['exit_code']==0 and not Path('/proc',str(leaf['pid'])).exists()
 assert leaf['stat'].split()[0]==str(leaf['pid']) and leaf['stat'].rsplit(')',1)[1].split()[19].isdigit()
 assert shared.sha(leaf['exe'])==leaf['exe_sha256'] and subprocess.check_output(['go','version','-m',leaf['exe']],text=True)==leaf['build_info']
 assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in leaf['build_info']
 assert leaf['argv'][0]==leaf['exe'] and Path(leaf['exe']).is_relative_to(proof_path.parent) and leaf['argv'][leaf['argv'].index('-n')+1]=='wf-process-0'
 hub_ids.update(peer['id'] for peer in proof['hubs'])
assert len(reports)==len({leaf['pid'] for leaf in leaves})==3 and len(hub_ids)==9
assert {report['test'] for report in reports}=={worker_leaf_wire.TEST+'/'+mode for mode in ('static','kv','auto')}
assert len(re.findall(r'worker domain admitted node=\d domain=WFWORKER server_id=\w+',log))==9
assert set(re.findall(r'worker domain admitted node=\d domain=WFWORKER server_id=(\w+)',log))==hub_ids
assert not (root/'leaf-wire-review.json').exists()
meta=read(meta_root/'archive-verification.json');inventory=read(meta_root/'fixture-inventory.json')
with root.with_suffix('.tar.gz').open('rb') as stream:
 declared,compressed=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
closure=shared.closure(root)
# Mutate copies of actual captured proof documents only; original stores never reopen.
first=next((root/'stores').glob('wf-worker-leaf-*/leaf-proof.json')).parent
base={name:read(first/name) for name in ('leaf-proof.json','traffic.json','proxy-final.json')}
mutations=[]
def variant(fn):
 value=copy.deepcopy(base);fn(value);mutations.append(value)
variant(lambda v:v['leaf-proof.json'].update(scenario_passed=False))
variant(lambda v:v['leaf-proof.json'].update(remote_domain='WFEDGE'))
variant(lambda v:v['leaf-proof.json'].update(local_domain='WFWORKER'))
variant(lambda v:v['leaf-proof.json'].update(local_streams_after=1))
variant(lambda v:v['leaf-proof.json']['hubs'][0].update(id=v['leaf-proof.json']['hubs'][1]['id']))
variant(lambda v:v['leaf-proof.json']['hubs'][0].update(name='wrong'))
variant(lambda v:v['leaf-proof.json']['leaf_before'].update(server_id='wrong'))
variant(lambda v:v['leaf-proof.json']['leaf_after']['leafs'][0].update(account='OTHER'))
variant(lambda v:v['traffic.json'].update(truncated=True))
variant(lambda v:v['traffic.json']['connections'][0].update(target='127.0.0.1:1'))
variant(lambda v:v['traffic.json']['connections'].append(copy.deepcopy(v['traffic.json']['connections'][0])))
variant(lambda v:v['proxy-final.json'].update(active_connections=1))
variant(lambda v:v['proxy-final.json'].update(buffered_bytes=1))
variant(lambda v:v['proxy-final.json'].update(buffer_overflows=1))
variant(lambda v:v['proxy-final.json'].update(client_to_server=v['proxy-final.json']['client_to_server']+1))
variant(lambda v:v['proxy-final.json'].update(server_to_client=v['proxy-final.json']['server_to_client']+1))
variant(lambda v:v['traffic.json']['frames'][0].update(connection=999))
variant(lambda v:v['traffic.json']['frames'][0].update(direction='unknown'))
import base64
def replace_wire(value,direction,old,new):
 found=False
 for frame in value['traffic.json']['frames']:
  if frame['direction']!=direction:continue
  data=base64.b64decode(frame['data']);changed=data.replace(old,new)
  if changed!=data:found=True
  frame['data']=base64.b64encode(changed).decode()
 assert found
variant(lambda v:replace_wire(v,'client_to_server',b'$JS.WFWORKER.API.',b'$JS.WRONGXXX.API.'))
variant(lambda v:replace_wire(v,'server_to_client',b'WFEDGE',b'WRONG!'))
variant(lambda v:replace_wire(v,'client_to_server',b'CONNECT ',b'UNKNOWN '))
with tempfile.TemporaryDirectory(prefix='worker-wire-review-controls-') as directory:
 target=Path(directory)
 for value in mutations:
  for name,document in value.items():(target/name).write_text(json.dumps(document))
  try:worker_leaf_wire.validate(target)
  except (AssertionError,ValueError,KeyError):pass
  else:raise AssertionError('actual wire proof mutation accepted')
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
report=dict(accepted=True,original_producer_exit=1,original_collector_failure_preserved=True,native_exit=0,verifier_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),verifier_sha256=shared.sha(repo/'scripts/worker_leaf_wire.py'),source=rev,source_files=len(expected),actual_sdk=sdk,actual_packaged_workers=children,actual_stock_leaves=leaves,hub_library_identities=sorted(hub_ids),wire_reports=reports,rejected_actual_proof_mutations=len(mutations),archive_files=len(inventory['files']),compressed_archive=compressed,fresh_closure=closure,original_stores_not_reopened=True,scope='Healthy full static/KV/auto packaged worker through real leaf with complete exclusive child transcripts. Embedded hub libraries and stock leaf processes. No fault interaction/daemon SQL/natural lost replies/full matrix qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
print('INDEPENDENT_WORKER_LEAF_ACCEPTED',rev,len(inventory['files']),len(mutations),reports,flush=True)
