"""Verify complete exclusive packaged-worker transcripts through a domain leaf."""
import base64
from collections import Counter
import json
from pathlib import Path

TEST='TestWorkerStandaloneCommandsThroughLeaf'


def protocol(data, incoming=False):
    """Parse framing so payload text cannot masquerade as protocol commands."""
    data=bytes(data)
    result=[];offset=0
    while offset<len(data):
        end=data.find(b'\r\n',offset)
        assert end>=offset,'incomplete protocol line'
        line=data[offset:end];offset=end+2;parts=line.split();assert parts
        op=parts[0];payload=None
        sizes={b'MSG':(4,5),b'HMSG':(5,6)} if incoming else {b'PUB':(3,4),b'HPUB':(4,5)}
        if op in sizes:
            assert len(parts) in sizes[op]
            assert parts[-1].isdigit();size=int(parts[-1])
            if op in (b'HPUB',b'HMSG'):
                assert parts[-2].isdigit() and 0<int(parts[-2])<=size
            assert offset+size+2<=len(data) and data[offset+size:offset+size+2]==b'\r\n','incomplete payload'
            payload=data[offset:offset+size];offset+=size+2
        elif op in (b'INFO',b'CONNECT'):
            assert op==(b'INFO' if incoming else b'CONNECT')
            payload=json.loads(line.split(b' ',1)[1]);assert isinstance(payload,dict)
        elif op in (b'PING',b'PONG',b'+OK'):
            assert len(parts)==1 and (incoming or op!=b'+OK')
        elif not incoming and op==b'SUB':assert len(parts) in (3,4)
        elif not incoming and op==b'UNSUB':assert len(parts) in (2,3)
        else:raise AssertionError('unexpected protocol command: '+repr(op))
        result.append((op,parts,payload))
    return result


def validate(root):
    root=Path(root);read=lambda name:json.loads((root/name).read_text())
    proof=read('leaf-proof.json');trace=read('traffic.json');stats=read('proxy-final.json')
    assert proof['scenario_passed'] is True
    assert proof['test'] in {TEST+'/'+mode for mode in ('static','kv','auto')}
    assert proof['remote_domain']=='WFWORKER' and proof['local_domain']=='WFEDGE'
    assert proof['local_streams_before']==proof['local_streams_after']==0
    hubs=proof['hubs'];assert len(hubs)==3
    assert {h['name'] for h in hubs}=={f'wf-test-{i}' for i in range(3)}
    assert len({h['id'] for h in hubs})==3 and all(h['domain']=='WFWORKER' for h in hubs)
    assert proof['leaf_id'] not in {h['id'] for h in hubs}
    for stage in ('leaf_before','leaf_after'):
        leaf=proof[stage]
        assert leaf['server_id']==proof['leaf_id'] and leaf['leafnodes']==len(leaf['leafs'])==1
        assert leaf['leafs'][0]['name'] in {h['name'] for h in hubs} and leaf['leafs'][0]['account']=='$G'
    assert trace['truncated'] is False and len(trace['connections'])==1
    connection=trace['connections'][0]
    assert connection['target']==proof['leaf_url'].removeprefix('nats://')
    assert stats['active_connections']==stats['buffered_bytes']==stats['buffer_overflows']==0
    streams={direction:bytearray() for direction in ('client_to_server','server_to_client')}
    for frame in trace['frames']:
        assert frame['connection']==connection['id'] and frame['direction'] in streams
        streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True))
    for direction,stream in streams.items():assert len(stream)==stats[direction]>0
    outgoing=protocol(streams['client_to_server']);incoming=protocol(streams['server_to_client'],True)
    assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1
    assert incoming[0][0]==b'INFO'
    infos=[body for op,_,body in incoming if op==b'INFO']
    assert infos and all(info['server_id']==proof['leaf_id'] and info.get('domain')=='WFEDGE' for info in infos)
    subjects=Counter(parts[1].decode('ascii') for op,parts,_ in outgoing if op in (b'PUB',b'HPUB'))
    api={name:count for name,count in subjects.items() if name.startswith('$JS.') and '.API.' in name}
    assert api and all(name.startswith('$JS.WFWORKER.API.') for name in api)
    assert any('STREAM.INFO.WF_JRN' in name for name in api)
    assert any('CONSUMER.' in name and 'WF_RUN' in name for name in api)
    return dict(test=proof['test'],leaf_pid=proof['leaf_pid'],leaf_id=proof['leaf_id'],
                client_bytes=len(streams['client_to_server']),server_bytes=len(streams['server_to_client']),
                domain_api_publications=sum(api.values()),api_subjects=api,
                scope='Complete observed exclusive child TCP transcript through a real leaf; framing parsed including payload boundaries. Healthy full worker smoke only, no fault/daemon/SQL/full matrix claim.')
