import sys,pathlib,json,hashlib,subprocess,shutil,base64,copy,importlib.util,datetime,re
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-native-blob-authority-v2-20261007');out=repo/'docs/scale/blob-native-authority-2026-10-07/accepted';out.mkdir(exist_ok=False)
sys.path.insert(0,str(repo/'scripts'));from worker_leaf_wire import protocol
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();read=lambda p:json.loads(p.read_text())
before=read(root/'source-before.json');assert before==read(root/'source-after.json');revision=before['revision']
for name,digest in before['files'].items():
 p=root/'selected-source'/name
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert sha(p)==digest==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()
actual=read(root/'actual-sdk.json');binary=read(root/'binary.json');execution=read(root/'execution.json');command=read(root/'commands.json')['run']
assert execution['exit_code']==0 and execution['source']==revision
assert actual['args']==command and actual['exe_sha256']==binary['sha256']==sha(root/'blobpublication-race.test');assert actual['admission']['stable_identity_observed_twice'] and not pathlib.Path('/proc',str(actual['pid'])).exists()
assert actual['environment']=={'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB','WF_BLOB_AUTHORITY_ROOT':str(root/'stores')};assert '-race=true' in binary['build_info'] and 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
log=(root/'actual.log').read_text();assert log.rstrip().endswith('PASS') and 'DATA RACE' not in log and '--- FAIL:' not in log and '--- SKIP:' not in log
assert log.count('stale CAS rejected')==2 and log.count('same-store restart retained fences')==2
assert len(re.findall(r'--- PASS: TestSeededPublicationLifecycle/\d+ ',log))==128

def check(d):
 replicas=d['replicas'];assert replicas in [1,3];stream=d['stream'];c=stream['config'];s=stream['state'];assert c['name']=='BLOB_AUTH' and c['subjects']==['wf.blob.authority.>'] and c['num_replicas']==replicas and c['storage']=='file' and c['retention']=='limits' and c['max_msgs_per_subject']==1
 assert c['deny_delete'] and c['deny_purge'] and not c.get('allow_direct',False) and not c.get('allow_msg_ttl',False) and c.get('max_age',0)==0 and c.get('max_msgs',-1)<=0 and c.get('max_bytes',-1)<=0
 assert s['messages']==s['num_subjects']==4 and s['consumer_count']==0
 assert d['retired_root']['Head']==3 and d['retired_root']['Token']=='' and not d['retired_root']['Blobs']
 assert d['next_generation']['Fence']['Generation']==2 and d['next_generation']['Fence']['Phase']=='uploading'
 w=d['late_wire'];held=w['held'];forwarded=w['forwarded'];packet=base64.b64decode(held['packet'],validate=True)
 assert held['disposition']=='held' and held['forwarded_bytes']==0 and held['subject'].startswith('wf.blob.authority.root.')
 assert forwarded['subject']==held['subject'] and forwarded['disposition']=='forwarded' and forwarded['forwarded_bytes']==len(packet)==333 and base64.b64decode(forwarded['packet'],validate=True)==packet
 assert b'Nats-Expected-Last-Subject-Sequence: 0\r\n' in packet
 assert w['fenced_root']['Head']==w['after_rejection']['Head']==1 and w['after_rejection']['Token']=='' and not w['after_rejection']['Blobs']
 trace=w['wire'];assert trace['truncated'] is False and len(trace['connections'])==1
 streams={x:bytearray() for x in ['client_to_server','server_to_client']}
 for frame in trace['frames']:assert frame['connection']==1;streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
 outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
 assert bytes(streams['client_to_server']).count(packet)==1
 assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1
 infos=[body for op,_,body in incoming if op==b'INFO'];assert infos and all(x['version']=='2.15.0' and x['server_name']=='wf-test-0' and x['jetstream'] for x in infos)
 publications=[(parts,body) for op,parts,body in outgoing if op in [b'PUB',b'HPUB'] and parts[1].decode()==held['subject']];assert len(publications)==1
 responses=[json.loads(body) for op,_,body in incoming if op==b'MSG' and body.startswith(b'{')]
 errors=[x['error'] for x in responses if x.get('error',{}).get('err_code') in [10071,10164]];assert len(errors)==1
 return {'replicas':replicas,'server_info':infos[0],'server_rejection':errors[0],'held_and_forwarded_bytes':len(packet),'wire_frames':len(trace['frames'])}
proofs={}
controls=[]
for p in (root/'stores').rglob('authority-proof.json'):
 d=read(p);proofs[str(d['replicas'])]=check(d);dest=out/('R'+str(d['replicas'])+'-authority-proof.json');shutil.copy2(p,dest)
 mutations=[('head_reset',lambda x:x['retired_root'].update(Head=0)),('generation_reuse',lambda x:x['next_generation']['Fence'].update(Generation=1)),('purge_allowed',lambda x:x['stream']['config'].update(deny_purge=False)),('direct_reads',lambda x:x['stream']['config'].update(allow_direct=True)),('wire_truncated',lambda x:x['late_wire']['wire'].update(truncated=True)),('packet_never_forwarded',lambda x:x['late_wire']['forwarded'].update(forwarded_bytes=0)),('published_despite_fence',lambda x:x['late_wire']['after_rejection'].update(Token='late')),('held_already_forwarded',lambda x:x['late_wire']['held'].update(forwarded_bytes=1))]
 for label,mutate in mutations:
  bad=copy.deepcopy(d);mutate(bad)
  try:check(bad)
  except (AssertionError,ValueError,KeyError):controls.append('R'+str(d['replicas'])+'-'+label)
  else:raise AssertionError('mutated proof accepted: '+label)
assert set(proofs)=={'1','3'}
review=dict(source=revision,source_inputs_verified=len(before['files']),actual_sdk=actual,binary=binary,execution=execution,proofs=proofs,actual_positive_proof_substitutions_rejected=controls,reviewed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Native R1/R3 authority component, exact late wire CAS, restart, unsafe config, bounded admission and standalone model/race tests. INFO git_commit is the embedding application revision, not an upstream NATS source revision. No ObjectStore, runtime workflow wiring, physical decoder, partition/lost-publish-reply, full124 or release qualification.')
(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
for name in ['source-before.json','source-after.json','actual-sdk.json','binary.json','execution.json','actual.log','executed-producer.py']:shutil.copy2(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(root.with_name(root.name+'-proof')/name,out/name)
print(json.dumps({'accepted':True,'actual_positive_controls':len(controls),'source_inputs':len(before['files']),'elapsed':execution['elapsed_seconds']}),flush=True)
