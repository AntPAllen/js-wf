"""Incrementally check closed disk-backed SQL projector wire; no payload-sized history."""
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path

class ProtocolStream:
    def __init__(self,incoming=False):
        self.incoming=incoming;self.buffer=bytearray();self.bytes=0;self.hash=hashlib.sha256();self.operations=Counter();self.api=Counter();self.infos=[];self.peak_buffer=0
    def feed(self,data):
        self.bytes+=len(data);self.hash.update(data);self.buffer.extend(data);self.peak_buffer=max(self.peak_buffer,len(self.buffer))
        while self.buffer:
            end=self.buffer.find(b'\r\n')
            if end<0:
                assert len(self.buffer)<=64<<10,'oversized control line';return
            assert end<=64<<10,'oversized control line'
            line=bytes(self.buffer[:end]);parts=line.split();assert parts
            op=parts[0];through=end+2
            sizes={b'MSG':(4,5),b'HMSG':(5,6)} if self.incoming else {b'PUB':(3,4),b'HPUB':(4,5)}
            if op in sizes:
                assert len(parts) in sizes[op] and parts[-1].isdigit()
                size=int(parts[-1]);assert size<=16<<20,'oversized payload'
                if op in (b'HPUB',b'HMSG'):assert parts[-2].isdigit() and 0<int(parts[-2])<=size
                through+=size+2
                if through>len(self.buffer):return
                assert self.buffer[through-2:through]==b'\r\n','invalid payload terminator'
                if not self.incoming and parts[1].startswith(b'$JS.') and b'.API.' in parts[1]:
                    subject=parts[1].decode('ascii');assert subject.startswith('$JS.WFVIEW.API.'),'wrong API domain'
                    self.api[subject]+=1
            elif op in (b'INFO',b'CONNECT'):
                assert op==(b'INFO' if self.incoming else b'CONNECT')
                value=json.loads(line.split(b' ',1)[1]);assert isinstance(value,dict)
                if op==b'INFO':self.infos.append(value)
            elif op in (b'PING',b'PONG',b'+OK'):assert len(parts)==1 and (self.incoming or op!=b'+OK')
            elif not self.incoming and op==b'SUB':assert len(parts) in (3,4)
            elif not self.incoming and op==b'UNSUB':assert len(parts) in (2,3)
            else:raise AssertionError('unexpected protocol command: '+repr(op))
            if not self.operations and self.incoming:assert op==b'INFO'
            self.operations[op.decode('ascii')]+=1;del self.buffer[:through]
    def finish(self):
        assert not self.buffer,'incomplete protocol tail'
        return dict(bytes=self.bytes,sha256=self.hash.hexdigest(),operations=dict(self.operations),api=dict(self.api),infos=self.infos,peak_buffer=self.peak_buffer)


def validate_phase(root,leaf_id=None):
    root=Path(root);trace=json.loads((root/'traffic.json').read_text());stats=json.loads((root/'proxy-final.json').read_text())
    assert trace['truncated'] is False and not trace.get('frame_file_error') and trace['frames'] in (None,[])
    assert trace['frame_file']=='traffic.frames.jsonl' and type(trace['frame_records']) is int and trace['frame_records']>0
    assert len(trace['connections'])==1
    assert all(type(stats[k]) is int and stats[k]==v for k,v in dict(accepted_connections=1,upstream_dial_failures=0,active_connections=0,buffered_bytes=0,buffer_overflows=0).items())
    streams={d:ProtocolStream(d=='server_to_client') for d in ('client_to_server','server_to_client')}
    count=0;encoded=hashlib.sha256();encoded_bytes=0
    with (root/trace['frame_file']).open('rb') as file:
        for line in file:
            assert len(line)<=1<<20 and line.endswith(b'\n'),'incomplete/oversized encoded frame'
            encoded.update(line);encoded_bytes+=len(line)
            frame=json.loads(line);assert type(frame['connection']) is int and frame['connection']==trace['connections'][0]['id'] and frame['direction'] in streams
            data=base64.b64decode(frame['data'],validate=True);assert data
            streams[frame['direction']].feed(data);count+=1
    assert count==trace['frame_records']
    result={direction:stream.finish() for direction,stream in streams.items()}
    for direction,stream in result.items():assert type(stats[direction]) is int and stream['bytes']==stats[direction]>0
    assert result['client_to_server']['operations'].get('CONNECT')==1 and result['client_to_server']['api']
    infos=result['server_to_client']['infos'];assert infos
    assert all(info.get('domain')=='WFEDGE' and (leaf_id is None or info['server_id']==leaf_id) for info in infos)
    assert len({info['server_id'] for info in infos})==1
    return dict(connection=trace['connections'][0],streams=result,encoded_bytes=encoded_bytes,encoded_sha256=encoded.hexdigest(),frame_records=count)


def validate_case(case,proof):
    case=Path(case);read=lambda p:json.loads(p.read_text())
    assert proof['projector_transport']=='leaf' and proof['count']==50000 and proof['native_test_failed'] is False
    leaf=read(case/'leaf-route/leaf-proof.json');process=read(case/'leaf-route/leaf.process.json')
    assert leaf['scenario_passed'] is True and leaf['remote_domain']=='WFVIEW' and leaf['local_domain']=='WFEDGE'
    assert leaf['local_streams_before']==leaf['local_streams_after']==0 and leaf['leaf_url']==proof['projector_leaf_url']
    assert leaf['leaf_pid']==process['pid'] and process['reaped'] is True and process['exit_code']==0
    assert process['argv'][0]==process['exe'] and hashlib.file_digest(Path(process['exe']).open('rb'),'sha256').hexdigest()==process['exe_sha256']
    assert not Path('/proc',str(process['pid'])).exists() and str(process['pid'])==process['stat'].split()[0]
    assert 'v2.15.0' in process['build_info']
    initial={h['name']:h['id'] for h in leaf['hubs']};after={h['name']:h['id'] for h in leaf['hubs_after']}
    assert len(leaf['hubs'])==len(leaf['hubs_after'])==3 and len(set(initial.values()))==len(set(after.values()))==3
    assert set(initial)==set(after)=={f'wf-test-{i}' for i in range(3)}
    assert all(h['domain']=='WFVIEW' for h in leaf['hubs']+leaf['hubs_after'])
    leader='wf-test-'+str(proof['journal_leader_node'])
    assert initial[leader]==proof['journal_leader_old_server_id'] and after[leader]==proof['journal_leader_new_server_id'] and initial[leader]!=after[leader]
    assert all(initial[h]==after[h] for h in initial if h!=leader)
    assert after=={'wf-test-'+h['node']:h['server_id'] for h in proof['healed_domain_peers']}
    assert leaf['leaf_id'] not in set(initial.values())|set(after.values())
    for stage in ('leaf_before','leaf_after'):
        topology=leaf[stage];assert topology['server_id']==leaf['leaf_id'] and topology['leafnodes']==len(topology['leafs'])==1
        assert topology['leafs'][0]['name'] in initial and topology['leafs'][0]['account']=='$G'
    records=proof['standalone_projectors'];assert [p['phase'] for p in records]==['initial','catchup','replacement']
    phases={}
    for p in records:
        root=case/('projector-'+p['phase']+'-leaf-wire')
        assert p['leaf_transport'] is True and p['nats_target']==leaf['leaf_url'] and p['nats_url']==p['argv'][2] and p['nats_url']!=p['nats_target'] and p['wire_root']==str(root)
        capture=validate_phase(root,leaf['leaf_id']);assert capture['connection']['target']==leaf['leaf_url'].removeprefix('nats://')
        assert capture['encoded_bytes']+1024<=512<<20
        phases[p['phase']]=capture
    return dict(phases=phases,actual_leaf=process,leaf_id=leaf['leaf_id'],client_bytes=sum(p['streams']['client_to_server']['bytes'] for p in phases.values()),server_bytes=sum(p['streams']['server_to_client']['bytes'] for p in phases.values()),forwarded_domain_api_publications=sum(sum(p['streams']['client_to_server']['api'].values()) for p in phases.values()),scope='Complete joined file-backed child wire for the original full50000 SQL fault/rebuild case through one actual stock leaf/R3 hubs; no leaf SIGKILL, process-killed NATS hubs, natural reply loss, broad matrix or release acceptance.')
