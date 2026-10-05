from pathlib import Path
import json,re,datetime,hashlib,shutil
r=Path('/tmp/js-wf-held-metadata-rollout-10m-launch-proof-20261005');out=Path('/home/exedev/js-wf/docs/scale/held-lease-observation-2026-10-05/10m-launch')
def ns(x):
 m=re.fullmatch(r'(.*?)(?:\.(\d+))?Z',x);d=datetime.datetime.fromisoformat(m[1]).replace(tzinfo=datetime.timezone.utc);return int(d.timestamp())*1000000000+int((m[2] or '').ljust(9,'0'))
snapshot=json.loads((r/'live-held-snapshot.json').read_text());ages=[]
for item in snapshot['records']:
 e=item['event'];o=e['HeldLease'];assert e['Stage']=='lease_held' and e['Error']=='workflow lease is held';assert o['key']==e['Type']+'.'+e['ID']
 age=ns(o['observed_at'])-ns(o['created']) if o['entry_observed'] else None;assert age==item['client_observed_minus_server_created_ns']
 if age is not None:ages.append(age)
assert len(snapshot['records'])==snapshot['held_records'];assert sum(a>=12000000000 for a in ages)==snapshot['observed_age_ge_configured_12s'];assert max(ages)==snapshot['max_observed_age_ns']
review={'launch':json.loads((r/'review.json').read_text()),'held_snapshot_records':len(snapshot['records']),'age_ge_12s':sum(a>=12000000000 for a in ages),'maximum_client_minus_server_age_ns':max(ages),'clock_difference_is_not_expiry_authority':True,'non_atomic_live_complete_records_not_final':True,'cause_confirmed':False,'terminal_pass':False}
(out/'independent-snapshot-review.json').write_text(json.dumps(review,indent=2)+'\n')
for n in ['review.json','live-held-snapshot.json','executed-capture.py']:shutil.copy2(r/n,out/n)
shutil.copy2(__file__,out/'executed-snapshot-review.py');shutil.copy2('/tmp/js-wf-watch-held-metadata-rollout-10m-20261005.py',out/'observer.py');print(json.dumps(review))
