"""Bind every packaged operator child to its complete exclusive leaf transcript."""
import base64
from collections import Counter
import json
from pathlib import Path
from worker_leaf_wire import protocol

TEST='TestOperatorStandaloneCommandsThroughLeaf'


def validate(stores):
    stores=Path(stores)
    read=lambda p:json.loads(p.read_text())
    leaves=list(stores.rglob('leaf-proof.json'));assert len(leaves)==1
    proof=read(leaves[0]);assert proof['scenario_passed'] is True and proof['test']==TEST
    assert proof['remote_domain']=='WFOPS' and proof['local_domain']=='WFEDGE'
    assert proof['local_streams_before']==proof['local_streams_after']==0
    hubs=proof['hubs'];assert len(hubs)==3 and len({h['id'] for h in hubs})==3
    assert {h['name'] for h in hubs}=={f'wf-test-{i}' for i in range(3)} and all(h['domain']=='WFOPS' for h in hubs)
    assert proof['leaf_id'] not in {h['id'] for h in hubs}
    for stage in ('leaf_before','leaf_after'):
        leaf=proof[stage]
        assert leaf['server_id']==proof['leaf_id'] and leaf['leafnodes']==len(leaf['leafs'])==1
        assert leaf['leafs'][0]['name'] in {h['name'] for h in hubs} and leaf['leafs'][0]['account']=='$G'
    processes=list(stores.rglob('standalone.process.json'));assert len(processes)==23
    rows=[]
    for path in processes:
        process=read(path);root=path.parent
        expected=read(root/'wire-expectation.json');trace=read(root/'traffic.json');stats=read(root/'proxy-final.json')
        assert process['domain']=='WFOPS' and process['argv'][1:]==expected['args']
        assert expected['args'][expected['args'].index('-url')+1]==expected['proxy_url']
        assert type(expected['offline']) is bool and expected['offline']==('-replay-bundle' in expected['args'])
        assert trace['truncated'] is False
        assert all(stats[k]==0 and type(stats[k]) is int for k in ('active_connections','buffered_bytes','buffer_overflows'))
        if expected['offline']:
            assert trace['connections'] in (None,[]) and trace['frames'] in (None,[])
            assert stats['client_to_server']==stats['server_to_client']==0
            rows.append(dict(pid=process['pid'],offline=True,client_bytes=0,server_bytes=0,domain_api_publications=0,api_subjects={}))
            continue
        assert len(trace['connections'])==1
        connection=trace['connections'][0];assert connection['target']==proof['leaf_url'].removeprefix('nats://')
        streams={key:bytearray() for key in ('client_to_server','server_to_client')}
        for frame in trace['frames']:
            assert frame['connection']==connection['id'] and frame['direction'] in streams
            streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
        for key,value in streams.items():assert len(value)==stats[key]>0
        outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
        assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1 and incoming[0][0]==b'INFO'
        infos=[body for op,_,body in incoming if op==b'INFO']
        assert infos and all(info['server_id']==proof['leaf_id'] and info.get('domain')=='WFEDGE' for info in infos)
        args=expected['args'];domain=args[args.index('-domain')+1];assert domain in ('WFOPS','MISSING')
        subjects=Counter(parts[1].decode('ascii') for op,parts,_ in outgoing if op in (b'PUB',b'HPUB'))
        api={key:value for key,value in subjects.items() if key.startswith('$JS.') and '.API.' in key}
        assert api and all(key.startswith('$JS.'+domain+'.API.') for key in api)
        if domain=='MISSING':assert process['exit_code']==1
        rows.append(dict(pid=process['pid'],offline=False,domain=domain,client_bytes=len(streams['client_to_server']),server_bytes=len(streams['server_to_client']),domain_api_publications=sum(api.values()),api_subjects=api))
    assert len({r['pid'] for r in rows})==23
    assert sum(r['offline'] for r in rows)==2
    assert sum(r.get('domain')=='MISSING' for r in rows)==1
    assert sum(r.get('domain')=='WFOPS' for r in rows)==20
    return dict(processes=rows,online_children=21,offline_children=2,leaf_id=proof['leaf_id'],leaf_pid=proof['leaf_pid'],client_bytes=sum(r['client_bytes'] for r in rows),server_bytes=sum(r['server_bytes'] for r in rows),domain_api_publications=sum(r['domain_api_publications'] for r in rows),scope='Full healthy packaged operator suite through one real leaf, including explicit missing-domain rejection and zero-connection offline replay. No daemon, SQL, injected fault or full-release acceptance.')
