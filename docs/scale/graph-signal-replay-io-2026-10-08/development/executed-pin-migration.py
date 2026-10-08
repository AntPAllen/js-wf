import hashlib,json,pathlib,subprocess
root=pathlib.Path.cwd()
base=root/'docs/scale/graph-signal-replay-io-2026-10-08/development'
captures=pathlib.Path('/home/exedev/js-wf-signal-replay-io-captures-20261008')
parent=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
body_hash=hashlib.sha256(b'true').hexdigest()
lineages=[]
prepared=[]
for p in sorted(captures.glob('*.json')):
 target=root/'sim/testdata/regressions'/('graph-signal-runtime-'+p.name)
 old_bytes=subprocess.check_output(['git','show',parent+':'+str(target.relative_to(root))])
 assert target.read_bytes()==old_bytes
 new_bytes=p.read_bytes()
 old,new=json.loads(old_bytes),json.loads(new_bytes)
 assert {k:v for k,v in old.items() if k!='transport'}=={k:v for k,v in new.items() if k!='transport'}
 remaining=iter(new['transport']);wanted=next(remaining,None);removed=[]
 for event in old['transport']:
  if wanted is not None and event==wanted:wanted=next(remaining,None)
  else:
   assert event['operation']=='graph_publication_get' and event['subject'].startswith(body_hash+'/') and event['sequence']==4 and event['outcome']=='ok',event
   removed.append(event)
 assert wanted is None and removed
 expected=380 if p.stem=='batch_restart' else 3 if p.stem=='consumption_unknown' else 2
 assert len(removed)==expected,(p.stem,len(removed))
 lineages.append({'path':str(target.relative_to(root)),'prior_source':parent,'seed':old['seed'],'mode':p.stem,'prior_sha256':hashlib.sha256(old_bytes).hexdigest(),'current_sha256':hashlib.sha256(new_bytes).hexdigest(),'removed_owned_queue_body_gets':len(removed),'seed_decisions_and_all_other_events_unchanged':True})
 prepared.append((target,new_bytes))
assert len(prepared)==18
# No migration is published until every lineage has been checked.
for target,body in prepared:target.write_bytes(body)
(base/'pin-lineage.json').write_text(json.dumps(lineages,indent=2)+'\n')
print('Verified 18 exact trace migrations; all other events and seed decisions unchanged.')
