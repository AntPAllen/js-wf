from pathlib import Path
import json,struct,hashlib,datetime,math,csv,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-scheduler-server-native-candidate-20261004/campaign')
out=repo/'docs/scale/scheduler-server-candidate-2026-10-04/compressed-outage-timing';out.mkdir(exist_ok=False)
m=json.loads((repo/'docs/scale/scheduler-server-candidate-2026-10-04/complete-proof/manifest.json').read_text())
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for name in ['report.json','receipts.bin']:
    entry=m['files']['native-failure/campaign/'+name];assert sha(root/name)==entry['sha256'] and (root/name).stat().st_size==entry['bytes'];shutil.copyfile(root/name,out/name)
r=json.loads((out/'report.json').read_text());raw=(out/'receipts.bin').read_bytes()
assert r['status']=='failed' and r['scheduled_count']==r['unique_received']==300 and r['horizon']=='1m30s'
def ns(value):
    assert value.endswith('Z');body=value[:-1];whole,dot,fraction=body.partition('.')
    assert not dot or fraction.isdigit() and 1<=len(fraction)<=9
    delta=datetime.datetime.strptime(whole,'%Y-%m-%dT%H:%M:%S')-datetime.datetime(1970,1,1)
    return (delta.days*86400+delta.seconds)*1000000000+int((fraction+'0'*9)[:9])
base=ns(r['FirstDue']);horizon=90000000000;assert ns(r['LastDue'])==base+horizon
cuts=[(ns(e['Started']),ns(e['Healed'])) for e in r['Restarts']];assert len(cuts)==2 and all(a<b for a,b in cuts)
assert len(raw)==300*40
rows=[];sequences=set();late=[]
for i in range(300):
    slot=raw[i*40:(i+1)*40];assert hashlib.sha256(slot[:24]).digest()[:16]==slot[24:]
    sequence,at,server=struct.unpack('<QQQ',slot[:24]);due=base+horizon//299*i+horizon%299*i//299
    assert sequence and sequence not in sequences and at>=due and server>=due;sequences.add(sequence)
    delay=at-due;overlap=[j+1 for j,(start,heal) in enumerate(cuts) if due<=heal and at>=start]
    rows.append(dict(index=i,sequence=sequence,due_ns=due,receipt_ns=at,server_ns=server,late_ns=delay,outage_intersections=';'.join(map(str,overlap))))
    if delay>2000000000:late.append((i,delay,overlap))
values=sorted(row['late_ns'] for row in rows);p99=values[math.ceil(.99*len(values))-1]/1e9;maximum=values[-1]/1e9
assert p99==r['P99LateSeconds'] and maximum==r['MaxLateSeconds']
assert len(late)==60 and all(x[2] for x in late)
with (out/'receipts.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
analysis=dict(original_status='failed',receipts=300,all_slots_and_checksums_verified=True,raw_p99_seconds=p99,raw_max_late_seconds=maximum,raw_p99_limit_seconds=2,raw_max_limit_seconds=30,over_two_seconds=len(late),over_two_seconds_fraction=len(late)/300,late_receipt_intervals_outside_both_recorded_outages=0,outages=[dict(index=j+1,start_ns=a,heal_ns=b,seconds=(b-a)/1e9,over_two_second_intersections=sum(j+1 in x[2] for x in late)) for j,(a,b) in enumerate(cuts)],source_proof_manifest_sha256=sha(repo/'docs/scale/scheduler-server-candidate-2026-10-04/complete-proof/manifest.json'),report_sha256=sha(out/'report.json'),receipt_ledger_sha256=sha(out/'receipts.bin'),proves_sole_causality=False,accepts_failed_run=False,qualifies_original_million=False,qualifies_production_server=False)
(out/'analysis.json').write_text(json.dumps(analysis,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-analysis.py')
print(json.dumps(analysis),flush=True)
