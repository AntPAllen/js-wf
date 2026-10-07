"""Check five actual packaged daemon leaf captures, including held startup packets."""
import base64
import json
from pathlib import Path
from worker_leaf_wire import protocol

TEST='TestOperatorStandaloneDaemonSignalsThroughLeaf'


def validate(stores):
    stores=Path(stores);read=lambda p:json.loads(p.read_text())
    leaves=list(stores.rglob('leaf-proof.json'));assert len(leaves)==1
    leaf=read(leaves[0]);assert leaf['test']==TEST and leaf['scenario_passed'] is True
    assert leaf['remote_domain']=='WFOPS' and leaf['local_domain']=='WFEDGE'
    assert leaf['local_streams_before']==leaf['local_streams_after']==0
    hubs=leaf['hubs'];assert len(hubs)==3 and len({h['id'] for h in hubs})==3
    assert {h['name'] for h in hubs}=={f'wf-test-{i}' for i in range(3)} and all(h['domain']=='WFOPS' for h in hubs)
    assert leaf['leaf_id'] not in {h['id'] for h in hubs}
    for phase in ('leaf_before','leaf_after'):
        info=leaf[phase];assert info['server_id']==leaf['leaf_id'] and info['leafnodes']==len(info['leafs'])==1
        assert info['leafs'][0]['name'] in {h['name'] for h in hubs} and info['leafs'][0]['account']=='$G'
    paths=list(stores.rglob('daemon.process.json'));assert len(paths)==5
    rows=[]
    for path in paths:
        p=read(path);trace=read(path.parent/'traffic.json');stats=read(path.parent/'proxy-final.json')
        assert p['test']==TEST+'/'+p['command']+'/'+p['stage'] and p['scenario_passed'] is True and p['reaped'] is True
        assert p['domain']=='WFOPS' and p['leaf_endpoint']==leaf['leaf_url']
        args=p['operator_args'];assert p['argv']==[p['exe']]+args and args[-1]==p['command']
        assert args[args.index('-domain')+1]=='WFOPS'
        assert p['exit_code']==(1 if p['stage']=='fatal' else 0)
        assert p.get('signal')==({'startup':'terminated','running':'interrupt'}.get(p['stage']))
        if p['stage']=='fatal':assert 'stream not found' in (path.parent/'stdout-stderr.log').read_text()
        assert trace['truncated'] is False and len(trace['connections'])==1
        assert stats['accepted_connections']==1 and stats['upstream_dial_failures']==0
        assert all(stats[k]==0 for k in ('active_connections','buffered_bytes','buffer_overflows'))
        connection=trace['connections'][0];assert connection['target']==leaf['leaf_url'].removeprefix('nats://')
        streams={direction:bytearray() for direction in ('client_to_server','server_to_client')}
        for frame in trace['frames']:
            assert frame['connection']==connection['id'] and frame['direction'] in streams
            streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
        for direction,stream in streams.items():assert len(stream)==stats[direction]>0
        outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
        assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1 and incoming[0][0]==b'INFO'
        infos=[body for op,_,body in incoming if op==b'INFO']
        assert infos and all(info['server_id']==leaf['leaf_id'] and info.get('domain')=='WFEDGE' for info in infos)
        api=[parts[1].decode('ascii') for op,parts,_ in outgoing if op in (b'PUB',b'HPUB') and parts[1].startswith(b'$JS.') and b'.API.' in parts[1]]
        assert all(subject.startswith('$JS.WFOPS.API.') for subject in api)
        pending=p['pending_api']
        if p['stage']=='startup':
            assert not api and pending['connection']==connection['id'] and pending['disposition']=='cancelled' and pending['forwarded_bytes']==0
            attempted=protocol(base64.b64decode(pending['packet'],validate=True))
            assert len(attempted)==1 and attempted[0][0] in (b'PUB',b'HPUB')
            assert attempted[0][1][1].decode('ascii')==pending['subject'] and pending['subject'].startswith('$JS.WFOPS.API.')
        else:assert pending is None and api
        rows.append(dict(pid=p['pid'],command=p['command'],stage=p['stage'],exit_code=p['exit_code'],forwarded_domain_api_publications=len(api),client_bytes=len(streams['client_to_server']),server_bytes=len(streams['server_to_client'])))
    assert len({row['pid'] for row in rows})==5
    assert {(row['command'],row['stage']) for row in rows}=={('project','startup'),('project','running'),('project','fatal'),('tombstone-loop','startup'),('tombstone-loop','running')}
    return dict(processes=rows,actual_standalone_processes=5,leaf_id=leaf['leaf_id'],scope='Packaged project/tombstone daemons through one real leaf: controlled pre-forward startup SIGTERM, running SIGINT, and fatal missing-source rejection. No SQL daemon, natural server/route fault, broader matrix or release acceptance.')
