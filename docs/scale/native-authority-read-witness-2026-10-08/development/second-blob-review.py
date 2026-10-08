import sys,pathlib,json,hashlib,subprocess,shutil,base64,copy,datetime,re
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-native-authority-witness-20261008')
out=repo/'docs/scale/native-authority-read-witness-2026-10-08/accepted/blob-component'
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive
from worker_leaf_wire import protocol
read=lambda p:json.loads(p.read_text())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');revision=before['revision']
for name,digest in before['files'].items():
 p=root/'selected-source'/name
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert sha(p)==digest==hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()
actual=read(root/'blob-actual-sdk.json');binary=read(root/'blob-binary.json');execution=read(root/'blob-execution.json');command=read(root/'blob-commands.json')['run']
assert execution['exit_code']==0 and execution['source']==revision
assert actual['args']==command and actual['exe_sha256']==binary['sha256']==sha(root/'blob-race.test')
assert actual['admission']['stable_identity_observed_twice'] and not pathlib.Path('/proc',str(actual['pid'])).exists()
assert actual['environment']=={'GOMAXPROCS':'2','GOMEMLIMIT':'512MiB','WF_BLOB_AUTHORITY_ROOT':str(root/'blob-stores'),'WF_NATIVE_SNAPSHOT_ROOT':str(root/'snapshot-stores')}
assert '-race=true' in binary['build_info'] and 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
assert 'github.com/nats-io/nats-server/v2\tv2.15.0' in binary['build_info']
assert command==[str(root/'blob-race.test'),'-test.run=.','-test.v','-test.count=1','-test.timeout=3m']
log=(root/'blob-actual.log').read_text();assert log.rstrip().endswith('PASS') and 'DATA RACE' not in log and '--- FAIL:' not in log and '--- SKIP:' not in log
assert len(re.findall(r'--- PASS: TestSeededPublicationLifecycle/\d+ ',log))==128
for test in ['TestNativeObjectPublicationLifecycle','TestNativePartialUploadPublicationFence','TestNativeUnknownChunksBlockCollection','TestNativeLateChunkAfterGenerationClosed','TestNativeCommittedMetadataReplyLost','TestNativeObjectSameStoreColdRestart','TestNativeObjectUnsafeConfiguration']:
 assert re.search(r'^--- PASS: '+test+r' \(',log,re.M)
manifest=read(root.with_name(root.name+'-proof')/'fixture-inventory.json')
meta=read(root.with_name(root.name+'-proof')/'archive-verification.json')
assert fixture_archive.inventory(root)==manifest['files']
with root.with_suffix('.tar.gz').open('rb') as stream:
 declared,digest=fixture_archive.verify_hashed_stream(stream,{'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']})
assert declared==manifest

def valid_reference(name, generation=None):
 fields=name.split('/');assert len(fields)==3 and re.fullmatch('[0-9a-f]{64}',fields[0]) and re.fullmatch('[A-Za-z0-9-]+',fields[2])
 assert int(fields[1])>0 and str(int(fields[1]))==fields[1]
 if generation is not None:assert int(fields[1])==generation
 return fields[0]

def check_config(info,replicas,expected_messages):
 c=info['config'];state=info['state'];bucket='RECOVERABLE_BLOB'
 assert c['name']=='OBJ_'+bucket and c['subjects']==['$O.'+bucket+'.C.>','$O.'+bucket+'.M.>']
 assert c['metadata']['js-wf-blob-format']=='recoverable-v1' and c['num_replicas']==replicas and c['storage']=='file' and c['retention']=='limits'
 assert c['allow_rollup_hdrs'] and c['deny_delete'] and not c.get('deny_purge',False)
 for k in ['allow_direct','allow_msg_ttl','no_ack','sealed','allow_atomic','allow_msg_schedules','discard_new_per_subject']:
  assert not c.get(k,False),k
 for k in ['max_age','subject_delete_marker_ttl']:
  assert c.get(k,0)==0,k
 for k in ['max_msgs','max_bytes','max_msgs_per_subject','max_msg_size']:
  assert c.get(k,0)<=0,k
 for k in ['mirror','sources','subject_transform','republish']:
  assert not c.get(k),k
 assert c.get('persist_mode','default')=='default'
 assert state['messages']==state['num_subjects']==expected_messages and state['consumer_count']==0

def check_wire(d,kind):
 held=d['held'];end=d['final_packet'];trace=d['wire']
 assert held['forwarded_bytes']==0 and held['disposition']=='held'
 assert trace['truncated'] is False and len(trace['connections'])==1
 packet=base64.b64decode(held['packet'],validate=True)
 streams={x:bytearray() for x in ['client_to_server','server_to_client']}
 for frame in trace['frames']:
  assert frame['connection']==1
  streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
 outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
 assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1
 infos=[body for op,_,body in incoming if op==b'INFO']
 assert infos and all(x['version']=='2.15.0' and x['server_name']=='wf-test-0' and x['jetstream'] for x in infos)
 assert all(x.get('git_commit')==revision[:7] for x in infos)
 assert end['subject']==held['subject'] and base64.b64decode(end['packet'],validate=True)==packet
 suffix=held['subject'].split('.')
 assert suffix[:2]==['$O','RECOVERABLE_BLOB'] and len(suffix)==4 and suffix[2]==('C' if kind=='late-chunk' else 'M')
 name=base64.urlsafe_b64decode(suffix[3]).decode();valid_reference(name,1)
 assert base64.urlsafe_b64encode(name.encode()).decode()==suffix[3]
 if kind=='paused-partial-crash':
  assert end['disposition']=='cancelled' and end['forwarded_bytes']==0 and bytes(streams['client_to_server']).count(packet)==0
 else:
  assert end['disposition']=='forwarded' and end['forwarded_bytes']==len(packet) and bytes(streams['client_to_server']).count(packet)==1
  if suffix[2]=='M':assert b'Nats-Expected-Last-Subject-Sequence: 0\r\n' in packet
 publications=[]
 for op,parts,body in outgoing:
  if op not in [b'PUB',b'HPUB'] or not parts[1].startswith(b'$O.RECOVERABLE_BLOB.'):
   continue
  if op==b'HPUB':body=body[int(parts[-2]):]
  publications.append((parts[1].decode(),body))
 chunks=[body for subject,body in publications if subject=='$O.RECOVERABLE_BLOB.C.'+suffix[3]]
 assert len(chunks)==3 and len(chunks[0])==len(chunks[1])==128*1024
 assert hashlib.sha256(b''.join(chunks)).hexdigest()==valid_reference(name)
 metadata=[json.loads(body) for subject,body in publications if subject=='$O.RECOVERABLE_BLOB.M.'+suffix[3]]
 if kind=='paused-partial-crash':assert metadata==[]
 else:
  assert len(metadata)==1
  m=metadata[0];assert m['name']==name and m['nuid']==suffix[3] and m['bucket']=='RECOVERABLE_BLOB' and m['chunks']==3
  assert m['size']==sum(map(len,chunks)) and m['digest']=='SHA-256='+base64.urlsafe_b64encode(hashlib.sha256(b''.join(chunks)).digest()).decode()
 replies=[json.loads(body) for op,_,body in incoming if op==b'MSG' and body.startswith(b'{')]
 errors=[x['error'] for x in replies if x.get('error',{}).get('err_code') in [10071,10164]]
 if kind=='paused-partial-resume':assert len(errors)==1
 else:assert not errors
 acks=[x for x in replies if x.get('stream')=='OBJ_RECOVERABLE_BLOB' and x.get('seq',0)>0 and not x.get('duplicate',False)]
 assert len(acks)==(4 if kind=='late-chunk' else 3)
 return dict(name=name,server_info=infos[0],chunk_bytes=sum(map(len,chunks)),held_packet_bytes=len(packet),native_cas_errors=errors,acknowledgments=len(acks),wire_frames=len(trace['frames']))

def check(d):
 scenario=d['scenario'];replicas=d.get('replicas',1);assert replicas in [1,3]
 assert d.get('all_chunks_reclaimed',scenario=='unsafe-object-config') is True
 if scenario=='unknown-chunk-control':
  assert d['opaque_subject']=='$O.RECOVERABLE_BLOB.C.opaque-foreign-id' and d['blocked_before_deletion'] is True
  return {'scenario':scenario,'replicas':replicas}
 if scenario=='unsafe-object-config':
  expected={'age','messages','bytes','chunks_per_subject','direct','memory','ttl','no_rollup','purge_denied','delete_allowed','wrong_format','async_persist','atomic','schedules'}
  assert set(d['rejected'])==expected and len(d['rejected'])==14
  assert d['missing_not_created'] and d['post_open_collection_blocked'] and d['server_rejected_rollup_without_purge']
  return {'scenario':scenario,'replicas':replicas}
 check_config(d['object_stream'],replicas,1 if scenario=='committed-metadata-reply-lost' else 2)
 if scenario=='shared-lifecycle':
  assert d['standard_reader_verified'] and d['all_roots_retired'] and d['payload_bytes']==390000
 elif scenario=='same-store-cold-restart':
  old=d['old_server_ids'];new=d['new_server_ids'];assert len(old)==len(new)==replicas and len(set(old+new))==2*replicas
  assert d['old_metadata_rejected'] and d['standard_reader_after_cold_restart']
  assert d['retired_root']['Head']==2 and d['retired_root']['Token']=='' and not d['retired_root']['Blobs']
  assert d['deleted_partial']['deleted'] is True and d['deleted_partial']['chunks']==0 and d['deleted_partial']['size']==0
  assert d['live_root']['Head']==1 and len(d['live_root']['Blobs'])==1
  for k,ref in d['live_root']['Blobs'].items():assert ref['Generation']==1 and valid_reference(ref['Object'],1)==k
 elif scenario in ['paused-partial-resume','paused-partial-crash','late-chunk','committed-metadata-reply-lost']:
  wire=check_wire(d,scenario)
  if scenario in ['paused-partial-resume','paused-partial-crash']:
   assert d['partial_before']['state']['messages']==3 and d['partial_before']['state']['num_subjects']==1
   assert d['expired_root']['Head']==1 and d['expired_root']['Token']=='' and not d['expired_root']['Blobs']
  if scenario in ['paused-partial-resume','paused-partial-crash','late-chunk']:
   m=d['deleted_partial'];assert m['name']==wire['name'] and m['deleted'] is True and m['chunks']==m['size']==0
   fresh=d['fresh_root'];assert fresh['Head']==1 and len(fresh['Blobs'])==1
   for k,ref in fresh['Blobs'].items():assert ref['Generation']==2 and valid_reference(ref['Object'],2)==k and ref['Object']!=wire['name']
  if scenario=='late-chunk':
   assert d['partial_before']['state']['messages']==0 and d['closed_generation']['Fence']['Generation']==1 and d['closed_generation']['Fence']['Phase']=='closed'
   assert d['late_orphan']['name']==wire['name'] and d['late_orphan']['chunks']==3 and not d['late_orphan'].get('deleted',False)
   assert d['fresh_root_survived_old_delete']
  if scenario=='committed-metadata-reply-lost':
   stats=d['held_reply_stats'];assert stats['responses_held'] and stats['held_bytes']>0 and stats['buffered_bytes']>0 and stats['buffer_overflows']==0
   assert d['committed_metadata']['name']==wire['name'] and d['committed_metadata']['chunks']==3 and not d['committed_metadata'].get('deleted',False)
   assert d['unpromoted_fence']['Fence']['Phase']=='uploading' and d['unpromoted_fence']['Fence']['Object']==''
   assert d['unpublished_root']['Head']==0 and d['unpublished_root']['Token']=='' and not d['unpublished_root']['Blobs']
   assert d['cancelled_before_reply']
  return dict(scenario=scenario,replicas=replicas,wire=wire)
 else:raise AssertionError('unknown scenario')
 return {'scenario':scenario,'replicas':replicas}


def check_kill(d):
 scenario=d['scenario'];mode=scenario.removeprefix('process-sigkill-');assert mode in ['upload-metadata','upload-root','collect-purge']
 replicas=d['replicas'];assert replicas in [1,3]
 child=d['child'];assert child['pid']>0 and child['pid']!=actual['pid'] and child['exe_sha256']==binary['sha256']
 assert child['args']==[str(root/'blob-race.test'),'-test.run=^TestNativeBlobProcessChild$','-test.v','-test.count=1','-test.timeout=30s']
 assert child['start_ticks'].isdigit() and int(child['start_ticks'])>0 and d['stable_child_identity_observed_twice'] is True
 assert child['environment']['GOMAXPROCS']=='2' and child['environment']['GOMEMLIMIT']=='512MiB' and child['environment']['WF_BLOB_PROCESS_MODE']==mode
 assert child['environment']['WF_BLOB_PROCESS_URL'].startswith('nats://127.0.0.1:')
 assert not pathlib.Path('/proc',str(child['pid'])).exists()
 assert d['death']==dict(signal=9,exit_code=-1,joined=True,proc_absent=True)
 assert d['all_chunks_reclaimed'] is True and d['fresh_root_survived_old_delete'] is True
 check_config(d['object_stream'],replicas,2)
 name=d['old_physical_name'];k=valid_reference(name,1)
 payload=b'process-kill-native'*17000;assert k==hashlib.sha256(payload).hexdigest()
 encoded=base64.urlsafe_b64encode(name.encode()).decode()
 meta_subject='$O.RECOVERABLE_BLOB.M.'+encoded;chunk_subject='$O.RECOVERABLE_BLOB.C.'+encoded
 root_subject='wf.blob.authority.root.'+hashlib.sha256(b'killed').hexdigest()
 purge_subject='$JS.API.STREAM.PURGE.OBJ_RECOVERABLE_BLOB'
 expected_subject={'upload-metadata':meta_subject,'upload-root':root_subject,'collect-purge':purge_subject}[mode]
 held=d['held'];end=d['final_packet'];packet=base64.b64decode(held['packet'],validate=True)
 assert held['subject']==end['subject']==expected_subject and held['disposition']=='held' and held['forwarded_bytes']==0
 assert end['disposition']=='cancelled' and end['forwarded_bytes']==0 and base64.b64decode(end['packet'],validate=True)==packet
 operations=protocol(packet);assert len(operations)==1
 op,parts,body=operations[0];assert op in [b'PUB',b'HPUB'] and parts[1].decode()==expected_subject
 if op==b'HPUB':body=body[int(parts[-2]):]
 held_value=json.loads(body)
 if mode=='upload-metadata':
  assert b'Nats-Expected-Last-Subject-Sequence: 0\r\n' in packet
  assert held_value['name']==name and held_value['nuid']==encoded and held_value['size']==len(payload) and held_value['chunks']==3
 elif mode=='upload-root':
  assert re.search(rb'Nats-Expected-Last-Subject-Sequence: [1-9][0-9]*\r\n', packet)
  assert b'Wf-Authority-Read-Witness: 1\r\n' not in packet
  assert held_value['schema']=='js-wf-blob-authority-v2' and held_value['kind']=='root' and held_value['identity']=='killed' and held_value['revision']==1
  assert held_value['root']['Head']==1 and held_value['root']['Token']=='transaction'
  assert held_value['root']['Blobs']=={k:dict(Generation=1,Object=name)}
 else:assert held_value=={'filter':chunk_subject}
 state=d['partial_before']['state'];expected_counts={chunk_subject:3}
 if mode!='upload-metadata':expected_counts[meta_subject]=1
 assert state['subjects']==expected_counts and state['messages']==sum(expected_counts.values()) and state['num_subjects']==len(expected_counts)
 fence=d['before_fence']['Fence'];assert fence['Generation']==1 and fence['Phase']=={'upload-metadata':'uploading','upload-root':'ready','collect-purge':'closed'}[mode]
 if mode=='upload-root':assert fence['Object']==name
 else:assert fence['Object']==''
 if mode=='collect-purge':
  assert not fence['Intents'] and d['before_metadata']['deleted'] is True and d['before_metadata']['chunks']==d['before_metadata']['size']==0
  assert d['retired_root']['Head']==2 and not d['retired_root']['Token'] and not d['retired_root']['Blobs']
 else:
  assert set(fence['Intents'])=={'transaction'} and fence['Intents']['transaction']['Root']=='killed' and fence['Intents']['transaction']['Expected']==0
  if mode=='upload-metadata':assert d['before_metadata'] is None
  else:assert d['before_metadata']['name']==name and d['before_metadata']['chunks']==3 and not d['before_metadata'].get('deleted',False)
 root_fence=d['fenced_root'];assert root_fence['Head']==(2 if mode=='collect-purge' else 1) and not root_fence['Token'] and not root_fence['Blobs']
 closed=d['closed_generation']['Fence'];assert closed['Generation']==1 and closed['Phase']=='closed' and not closed['Object'] and not closed['Intents']
 deleted=d['deleted_partial'];assert deleted['name']==name and deleted['deleted'] is True and deleted['size']==deleted['chunks']==0
 fresh=d['fresh_root'];assert fresh['Head']==1 and len(fresh['Blobs'])==1 and set(fresh['Blobs'])=={k}
 ref=fresh['Blobs'][k];assert ref['Generation']==2 and valid_reference(ref['Object'],2)==k and ref['Object']!=name
 final_subjects=d['object_stream']['state']['subjects'];fresh_subject='$O.RECOVERABLE_BLOB.M.'+base64.urlsafe_b64encode(ref['Object'].encode()).decode()
 assert final_subjects=={meta_subject:1,fresh_subject:1}
 trace=d['wire'];assert trace['truncated'] is False and len(trace['connections'])==1
 streams={x:bytearray() for x in ['client_to_server','server_to_client']}
 for frame in trace['frames']:
  assert frame['connection']==1;streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
 assert bytes(streams['client_to_server']).count(packet)==0
 outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
 assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1
 infos=[body for op,_,body in incoming if op==b'INFO']
 assert infos and all(info['version']=='2.15.0' and info['git_commit']==revision[:7] and info['server_name']=='wf-test-0' and info['jetstream'] for info in infos)
 pubs=[]
 for op,parts,body in outgoing:
  if op not in [b'PUB',b'HPUB']:continue
  if op==b'HPUB':body=body[int(parts[-2]):]
  pubs.append((parts[1].decode(),body))
 assert not any(subject==expected_subject for subject,body in pubs)
 if mode=='collect-purge':
  tombstones=[json.loads(body) for subject,body in pubs if subject==meta_subject]
  assert len(tombstones)==1 and tombstones[0]['deleted'] is True and tombstones[0]['nuid']==encoded and tombstones[0]['chunks']==tombstones[0]['size']==0
 else:
  chunks=[body for subject,body in pubs if subject==chunk_subject]
  assert len(chunks)==3 and b''.join(chunks)==payload and len(chunks[0])==len(chunks[1])==128*1024
  if mode=='upload-root':
   metadata=[json.loads(body) for subject,body in pubs if subject==meta_subject];assert len(metadata)==1 and metadata[0]['chunks']==3 and metadata[0]['name']==name
 replies=[json.loads(body) for op,_,body in incoming if op==b'MSG' and body.startswith(b'{')]
 object_acks=[x for x in replies if x.get('stream')=='OBJ_RECOVERABLE_BLOB' and x.get('seq',0)>0 and not x.get('duplicate',False)]
 assert len(object_acks)=={'upload-metadata':3,'upload-root':4,'collect-purge':1}[mode]
 assert not any(x.get('error',{}).get('err_code') in [10071,10164] for x in replies)
 return dict(scenario=scenario,replicas=replicas,child_pid=child['pid'],child_sha256=child['exe_sha256'],native_exit_signal=9,held_packet_bytes=len(packet),object_acknowledgments=len(object_acks),server_info=infos[0],wire_frames=len(trace['frames']))

proofs={};controls=[];proof_paths=[p for p in (root/'blob-stores').rglob('object-proof.json') if not read(p)['scenario'].startswith(('authority-read-witness','read-witness-'))];assert len(proof_paths)==20
for p in proof_paths:
 d=read(p);identity=read(p.parent/'object-fixture-identity.json');replicas=d.get('replicas',1)
 assert identity['replicas']==replicas and identity['parent_budget_seconds']==30
 assert len(identity['peers'])==len(set(x['id'] for x in identity['peers']))==replicas
 assert {x['name'] for x in identity['peers']}=={f'wf-test-{i}' for i in range(replicas)}
 assert all(x['version']=='2.15.0' and x['embedding_commit']==revision[:7] for x in identity['peers'])
 label=p.parent.name;validate=check_kill if d['scenario'].startswith('process-sigkill-') else check
 proofs[label]=dict(validate(d),identity=identity)
 changes=[]
 if 'object_stream' in d:
  changes += [('chunks_remain',lambda x:x['object_stream']['state'].update(messages=100)),('direct_reads',lambda x:x['object_stream']['config'].update(allow_direct=True)),('ttl',lambda x:x['object_stream']['config'].update(max_age=1)),('rollup_disabled',lambda x:x['object_stream']['config'].update(allow_rollup_hdrs=False))]
  changes += [('reclamation_false',lambda x:x.update(all_chunks_reclaimed=False))]
 if 'held' in d:
  changes += [('already_forwarded',lambda x:x['held'].update(forwarded_bytes=1)),('truncated_wire',lambda x:x['wire'].update(truncated=True)),('wrong_packet_subject',lambda x:x['held'].update(subject='$O.RECOVERABLE_BLOB.M.Zm9yZWlnbg=='))]
 if 'fresh_root' in d:
  changes += [('generation_reused',lambda x:next(iter(x['fresh_root']['Blobs'].values())).update(Generation=1)),('tombstone_absent',lambda x:x['deleted_partial'].update(deleted=False))]
 if d['scenario']=='committed-metadata-reply-lost':
  changes += [('no_reply_hold',lambda x:x['held_reply_stats'].update(held_bytes=0)),('ambiguous_upload_adopted',lambda x:x['unpromoted_fence']['Fence'].update(Phase='ready')),('root_published',lambda x:x['unpublished_root'].update(Head=1))]
 if d['scenario']=='same-store-cold-restart':
  changes += [('same_server_instances',lambda x:x.update(new_server_ids=x['old_server_ids'])),('head_reset',lambda x:x['retired_root'].update(Head=0)),('cold_tombstone_missing',lambda x:x['deleted_partial'].update(deleted=False))]
 if d['scenario']=='unknown-chunk-control':changes += [('unknown_not_blocked',lambda x:x.update(blocked_before_deletion=False))]
 if d['scenario']=='unsafe-object-config':changes += [('unsafe_accepted',lambda x:x.update(rejected=x['rejected'][:-1])),('unsafe_mutation_allowed',lambda x:x.update(post_open_collection_blocked=False))]
 if d['scenario'].startswith('process-sigkill-'):
  changes += [('wrong_child_binary',lambda x:x['child'].update(exe_sha256='0'*64)),('not_sigkill',lambda x:x['death'].update(signal=15)),('child_not_joined',lambda x:x['death'].update(joined=False)),('child_still_present',lambda x:x['death'].update(proc_absent=False)),('identity_unstable',lambda x:x.update(stable_child_identity_observed_twice=False)),('held_packet_forwarded',lambda x:x['final_packet'].update(disposition='forwarded',forwarded_bytes=1)),('root_fence_reset',lambda x:x['fenced_root'].update(Head=0)),('generation_reset',lambda x:x['closed_generation']['Fence'].update(Generation=0)),('partial_census_wrong',lambda x:x['partial_before']['state'].update(messages=0)),('chunks_in_final_census',lambda x:x['object_stream']['state']['subjects'].update({'$O.RECOVERABLE_BLOB.C.foreign':1}))]
 for name,change in changes:
  bad=copy.deepcopy(d);change(bad)
  try:validate(bad)
  except (AssertionError,ValueError,KeyError):controls.append(label+'/'+name)
  else:raise AssertionError('actual-proof mutation accepted: '+label+'/'+name)
expected={('process-sigkill-upload-metadata',1),('process-sigkill-upload-metadata',3),('process-sigkill-upload-root',1),('process-sigkill-upload-root',3),('process-sigkill-collect-purge',1),('process-sigkill-collect-purge',3),('shared-lifecycle',1),('shared-lifecycle',3),('paused-partial-resume',1),('paused-partial-resume',3),('paused-partial-crash',1),('paused-partial-crash',3),('late-chunk',1),('late-chunk',3),('committed-metadata-reply-lost',1),('committed-metadata-reply-lost',3),('same-store-cold-restart',1),('same-store-cold-restart',3),('unknown-chunk-control',1),('unsafe-object-config',1)}
assert {(x['scenario'],x['replicas']) for x in proofs.values()}==expected
assert len(controls)>=200
assert len({x['child_pid'] for x in proofs.values() if 'child_pid' in x})==6
for p in proof_paths:
 if read(p)['scenario'].startswith('process-sigkill-'):
  child_log=(p.parent/'child.log').read_text();assert '=== RUN   TestNativeBlobProcessChild' in child_log and 'DATA RACE' not in child_log and 'FAIL' not in child_log
out.mkdir(parents=True,exist_ok=False)
for p in proof_paths:
 dest=out/'native-proofs'/p.parent.name;dest.mkdir(parents=True)
 shutil.copy2(p,dest/p.name);shutil.copy2(p.parent/'object-fixture-identity.json',dest/'object-fixture-identity.json')
 if (p.parent/'child.log').exists():shutil.copy2(p.parent/'child.log',dest/'child.log')
review=dict(source=revision,source_inputs_verified=len(before['files']),actual_sdk=actual,binary=binary,execution=execution,proofs=proofs,actual_positive_proof_substitutions_rejected=controls,complete_archive_verified=digest,reviewed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Experimental isolated full blob protocol native port including R1/R3 actual uploader/collector SIGKILL subprocesses at metadata/root/pre-purge boundaries: standard reads/sharing/retirement, R1/R3 partial metadata fences, late chunks/new generation, committed metadata with caller reply loss, all-peer same-store graceful cold restart, unknown chunks and native unsafe configurations. Crash mode is controlled connection loss, not process SIGKILL. INFO/Varz commits bind the embedding application, not separately captured upstream code. Selected source/actual embedded SDK/media/archive binding; external module files not independently captured. No runtime migration/history wiring, privileged permissions, native server process kill/power loss, arbitrary partition/lost-root-response/full124/matrix/release/online-GC/default-adoption qualification.')
(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
for name in ['source-before.json','source-after.json','blob-actual-sdk.json','blob-binary.json','blob-commands.json','blob-execution.json','blob-actual.log','executed-producer.py','executed-closure.py','closure.json']:
 shutil.copy2(root/name,out/name)
for name in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(root.with_name(root.name+'-proof')/name,out/name)
print(json.dumps({'accepted':True,'actual_positive_controls':len(controls),'source_inputs':len(before['files']),'elapsed':execution['elapsed_seconds'],'native_scenarios':len(proofs)}),flush=True)
