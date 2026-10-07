import sys,pathlib,json,hashlib,subprocess,shutil,re,copy,datetime
sys.dont_write_bytecode=True
repo=pathlib.Path('/home/exedev/js-wf');root=pathlib.Path('/tmp/js-wf-blob-publication-tier1-20261007');out=repo/'docs/scale/blob-publication-tier1-2026-10-07';out.mkdir(exist_ok=False)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
read=lambda p:json.loads(p.read_text())
before=read(root/'source-before.json');assert before==read(root/'source-after.json');revision=before['revision'];assert revision=='1c4e4f4'+revision[7:]
for name,fingerprint in before['files'].items():
 source=root/'selected-source'/name
 if source.suffix=='.go':source=source.with_suffix('.go.txt')
 assert sha(source)==fingerprint
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',revision+':'+name],cwd=repo)).hexdigest()==fingerprint
modes={'paused_commit','paused_upload','lost_upload_reply','lost_commit_reply','lost_fence_reply','lost_close_reply','lost_delete_reply','lost_pin_reply','lost_ready_reply','shared_reference','preserve_root','close_race','reset_head_control','drop_before_commit','partial_prepare'}
pins=sorted(p.name for p in (repo/'sim/testdata/regressions').glob('blob-publication-*.json'));assert len(pins)==15

def check(log,seeds):
 assert not any(x in log for x in ['DATA RACE','--- FAIL:','--- SKIP:'])
 assert re.findall(r'^PASS$',log,re.M)==['PASS']
 body=re.findall(r'TIER1_SEEDS test=TestSeededBlobPublicationReplay first=(\d+) last=(\d+) completed=(\d+) requested=(\d+)',log)
 assert body==[('1',str(seeds),str(seeds),str(seeds))]
 passing=re.findall(r'^--- PASS: (\w+) \(',log,re.M);assert sorted(passing)==sorted(['TestSeededBlobPublicationReplay','TestBlobPublicationTransportAuthorityCopies','TestPinnedRegressionCorpus'])
 pinpasses=re.findall(r'^\s+--- PASS: TestPinnedRegressionCorpus/(blob-publication-[\w-]+\.json) \(',log,re.M);assert sorted(pinpasses)==pins
 summaries=re.findall(r'blob publication: modes=map\[(.*?)\] every generated trace exactly replayed; referenced bytes and terminal reclamation checked; unsafe head reset detected',log);assert len(summaries)==1
 pairs=[token.split(':') for token in summaries[0].split()];counts={name:int(count) for name,count in pairs};assert len(pairs)==len(counts)==15 and set(counts)==modes and min(counts.values())>0 and sum(counts.values())==seeds
 coverages=re.findall(r'^TIER1_COVERAGE (.*)$',log,re.M);assert len(coverages)==1
 coverage=dict(x.split('=',1) for x in coverages[0].split());assert coverage['model_version']=='3' and coverage['seeds_per_workload']==str(seeds) and coverage['generated_schedules']==str(seeds) and coverage['scheduler_choices']==str(seeds*6)
 assert int(coverage['transport_events'])>0 and int(coverage['virtual_ms_max'])==8000 and coverage['under_1m']==str(seeds)
 assert all(coverage[x]=='0' for x in ['virtual_buckets_zero','under_1s','at_least_1m'])
 return dict(modes=counts,coverage=coverage,pins=len(pinpasses),contiguous_completed_bodies=seeds)

reviews={}
for profile,seeds,race in [('normal100k',100000,False),('race1000',1000,True)]:
 run=root/profile;execution=read(run/'execution.json');actual=read(run/'actual-sdk.json');binary=read(run/'binary.json');assert execution['exit_code']==0 and execution['seeds']==seeds and execution['race']==race
 assert actual['exe_sha256']==sha(run/'sim.test')==binary['sha256'];assert actual['args']==execution['command'] and actual['working_directory']==str(repo/'sim')
 assert actual['environment']['SIM_SEEDS']==str(seeds) and actual['environment']['GOMAXPROCS']=='2' and actual['environment']['GOMEMLIMIT']=='512MiB';assert actual['admission']['stable_identity_observed_twice']
 assert ('-race=true' in binary['build_info'])==race and 'vcs.revision='+revision in binary['build_info'] and 'vcs.modified=false' in binary['build_info'];assert not pathlib.Path('/proc',str(actual['pid'])).exists()
 log=(run/'actual.log').read_text();result=check(log,seeds);negatives=[]
 def reject(label,changed):
  try:check(changed,seeds)
  except (AssertionError,KeyError,ValueError):negatives.append(label);return
  raise AssertionError('mutated proof accepted: '+label)
 reject('missing_PASS',log.replace('\nPASS\n','\n'));reject('duplicate_PASS',log+'PASS\n');reject('native_failure',log+'--- FAIL: injected (0.00s)\n');reject('data_race',log+'DATA RACE\n')
 for field in ['last','completed','requested']:reject('short_'+field,log.replace(field+'='+str(seeds),field+'='+str(seeds-1)))
 reject('missing_body_counter',re.sub(r'^.*TIER1_SEEDS.*\n','',log,flags=re.M));reject('missing_coverage',re.sub(r'^TIER1_COVERAGE.*\n','',log,flags=re.M))
 reject('wrong_choices',log.replace('scheduler_choices='+str(seeds*6),'scheduler_choices='+str(seeds*5)))
 reject('missing_pin',re.sub(r'^\s+--- PASS: TestPinnedRegressionCorpus/'+re.escape(pins[0])+r' .*\n','',log,flags=re.M))
 first_count=result['modes']['close_race'];reject('mode_count_mismatch',log.replace('close_race:'+str(first_count),'close_race:'+str(first_count-1)))
 reviews[profile]=dict(verification=result,execution=execution,actual_sdk=actual,binary=binary,actual_log_sha256=sha(run/'actual.log'),actual_positive_mutations_rejected=negatives)
 for name in ['actual.log','execution.json','actual-sdk.json','binary.json']:
  dest=out/profile/name;dest.parent.mkdir(exist_ok=True);shutil.copy2(run/name,dest)
review=dict(source=revision,source_inputs_verified=len(before['files']),profiles=reviews,original_producer_format_failure=read(root/'original-producer-format-failure.json'),full124_graph_qualified=False,native_transport_qualified=False,production_online_gc_enabled=False,scope='Focused common Tier1 workload source, exact replay, 15 pinned traces and transport copying/CAS authority controls; not full graph or native online GC.')
(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,out/'executed-review.py')
for n in ['source-before.json','source-after.json','executed-producer.py','executed-continuation.py','original-producer-format-failure.json']:shutil.copy2(root/n,out/n)
proof=root.with_name(root.name+'-proof')
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(proof/n,out/n)
print(json.dumps({'focused_profiles_accepted':True,'source_inputs_verified':len(before['files']),'negative_controls':24}),flush=True)
