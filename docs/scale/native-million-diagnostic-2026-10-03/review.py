"""Independent review of run37059396897's immutable diagnostic archive."""
import hashlib,json,struct,subprocess,sys,tarfile
from datetime import datetime,timezone
from pathlib import Path

root=Path(sys.argv[1]); archive=next(root.rglob('native-million-originals.tar.gz'))
manifest=json.loads(next(root.rglob('native-million-manifest.json')).read_text())['files']
def ns(s):
    whole,fraction=s.removesuffix('Z').split('.')
    return int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp())*10**9+int(fraction.ljust(9,'0'))
with tarfile.open(archive) as t:
    names=t.getnames(); assert len(names)==len(set(names))==len(manifest)
    for m in t.getmembers():
        assert m.isfile(); h=hashlib.sha256(); f=t.extractfile(m)
        while chunk:=f.read(1024*1024): h.update(chunk)
        assert h.hexdigest()==manifest[m.name],m.name
    def read(n): return t.extractfile(n).read()
    def obj(n): return json.loads(read(n))
    source=obj('source.json'); revision='885664e3ec7b9c1b953d0b6313dabdf3c13bfe28'
    assert source['revision']==revision
    for name,digest in source['files'].items():
        assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+name])).hexdigest()==digest,name
    assert hashlib.sha256(read('wf-timer-volume')).hexdigest()==read('binary.sha256').decode().split()[0]
    r=obj('campaign/report.json'); count=1_000_000; horizon=600*10**9
    assert r['revision']==revision and r['source_modified']=='false' and r['status']=='passed'
    assert [r[k] for k in ('scheduled_count','acknowledged_publishes','unique_received')]==[count]*3
    assert r['horizon']=='10m0s' and r['lead']=='15m0s' and r['publishers']==64
    assert r['storage']=='file' and r['replicas']==3 and r['partitions']==64
    assert r['P99Limit']=='30s' and r['MaxLateLimit']=='1m0s'
    base=ns(r['FirstDue']); assert ns(r['LastDue'])==base+horizon
    ledger=read('campaign/receipts.bin'); observations=read('campaign/observations.bin')
    assert len(ledger)==40*count and len(observations)==16*count
    assert hashlib.sha256(observations).hexdigest()==r['observations_sha256']
    seen=set(); late=[]
    for i in range(count):
        slot=ledger[40*i:40*i+40]; seq,at,server=struct.unpack('<Qqq',slot[:24])
        assert hashlib.sha256(slot[:24]).digest()[:16]==slot[24:]
        assert seq>0 and seq not in seen; seen.add(seq)
        assert (seq,at)==struct.unpack_from('<Qq',observations,16*i)
        due=base+horizon//(count-1)*i+(horizon%(count-1)*i)//(count-1)
        assert at>=due and server>=due
        late.append((at-due)/10**9)
    late.sort(); p99=late[(99*count+99)//100-1]; maximum=late[-1]
    assert abs(p99-r['P99LateSeconds'])<=1e-9 and abs(maximum-r['MaxLateSeconds'])<=1e-9
    assert p99<=30 and maximum<=60
    restarts=r['Restarts']; assert len(restarts)==2
    for i,e in enumerate(restarts):
        assert ns(e['Started'])>=base+horizon*(i+1)//3 and ns(e['Healed'])>=ns(e['Started'])
        assert len(e['Killed'])==3 and len({k['PID'] for k in e['Killed']})==3
        assert all(k['PID']>0 and k['Signal']=='SIGKILL' for k in e['Killed'])
        assert 0<e['ReceivedBefore']<count
        if i: assert ns(e['Started'])>=ns(restarts[i-1]['Healed']) and e['ReceivedBefore']>restarts[i-1]['ReceivedBefore']
    logical=r['last_drain_audit']; assert logical['stream_messages']==logical['consumer_pending']==0 and logical['consumers_checked']==64
    assert logical==json.loads(read('campaign/drain-audits.jsonl').splitlines()[-1])
    physical=r['last_physical_drain_audit']; assert physical==json.loads(read('campaign/physical-drain-audits.jsonl').splitlines()[-1])
    assert physical['version']==1 and not physical.get('error') and len(physical['replicas'])==3
    ids=set(); lasts=set()
    for i,s in enumerate(physical['replicas']):
        assert s['node']==i and ns(s['observed'])>=ns(physical['at'])
        response=s['monitoring_response']; identity=response['server_id']; assert identity and identity not in ids; ids.add(identity)
        streams=[s for a in response['account_details'] for s in a['stream_detail'] if s['name']=='WF_RUN']; assert len(streams)==1
        s=streams[0]; state=s['state']; assert state['messages']==0 and state['consumer_count']==64 and state['last_seq']>0; lasts.add(state['last_seq'])
        consumers=s['consumer_detail']; assert len(consumers)==64 and {c['name'] for c in consumers}=={f'volume-{p}' for p in range(64)}
        assert all(c['num_pending']==c['num_ack_pending']==0 for c in consumers)
    assert len(lasts)==1
    assert read('offline-verification.log').decode().strip()=='campaign observations and gates verified'
    result=dict(run=37059396897,revision=revision,archive_members_verified=len(names),source_files_verified=len(source['files']),receipt_checksums_verified=count,unique_not_early_receipts=count,p99_seconds=p99,max_seconds=maximum,physical_replicas=3,physical_last_sequence=next(iter(lasts)),redeliveries=r['redeliveries'],ack_errors=r['ack_errors'],fetch_errors=r['fetch_errors'],last_fetch_error=r.get('last_fetch_error'),hosted_offline_verification=True,local_archived_binary_executed=False,scope='Accepted million-population ten-minute diagnostic with 30s/60s limits; not original 24h/2s release gate, nor explanation of original drain failure.')
    print(json.dumps(result,indent=2))
