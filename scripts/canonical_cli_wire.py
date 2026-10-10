"""Frame-aware complete compiled canonical CLI wire review."""
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path

class ProtocolStream:
    def __init__(self,incoming=False,api_prefix=None):
        self.legacy_purges=[];self.api_prefix=api_prefix;self.incoming=incoming;self.buffer=bytearray();self.bytes=0;self.hash=hashlib.sha256();self.operations=Counter();self.api=Counter();self.infos=[];self.peak_buffer=0
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
                    subject=parts[1].decode('ascii');assert self.api_prefix is not None and subject.startswith(self.api_prefix),'wrong API domain'
                    self.api[subject]+=1
                    if subject.endswith('.STREAM.PURGE.WF_JRN'):
                        assert size<=64<<10
                        payload=bytes(self.buffer[end+2:end+2+size])
                        if op==b'HPUB':payload=payload[int(parts[-2]):]
                        def unique(pairs):
                            result={}
                            for key,value in pairs:
                                assert key not in result,'duplicate purge field'
                                result[key]=value
                            return result
                        self.legacy_purges.append(json.loads(payload,object_pairs_hook=unique))
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
    def finish(self,allow_tail=False):
        tail=bytes(self.buffer)
        if tail and allow_tail:
            tokens=(b'MSG ',b'HMSG ',b'INFO ',b'PING',b'PONG',b'+OK') if self.incoming else (b'PUB ',b'HPUB ',b'SUB ',b'UNSUB ',b'PING',b'PONG')
            assert any(tail.startswith(token) or token.startswith(tail) for token in tokens),'unrecognized interrupted packet'
        assert allow_tail or not self.buffer,'incomplete protocol tail'
        return dict(legacy_purges=self.legacy_purges,bytes=self.bytes,sha256=self.hash.hexdigest(),operations=dict(self.operations),api=dict(self.api),infos=self.infos,peak_buffer=self.peak_buffer,interrupted_tail_bytes=len(tail),interrupted_tail_sha256=hashlib.sha256(tail).hexdigest())


def validate(root,domain,offline=False,legacy_purge_subject=None):
    root=Path(root)
    trace=json.loads((root/'traffic.json').read_text())
    stats=json.loads((root/'proxy-final.json').read_text())
    assert trace['truncated'] is False and not trace.get('frame_file_error')
    assert trace['frame_file']=='traffic.frames.jsonl' and trace['frames'] in (None,[])
    assert all(type(stats[k]) is int and stats[k]==0 for k in ('active_connections','buffered_bytes','buffer_overflows','upstream_dial_failures'))
    expected=0 if offline else 1
    assert type(stats['accepted_connections']) is int and stats['accepted_connections']==expected and len(trace['connections'] or [])==expected
    prefix='$JS.'+domain+'.API.' if domain else '$JS.API.'
    streams={d:ProtocolStream(d=='server_to_client',prefix) for d in ('client_to_server','server_to_client')}
    count=0
    with (root/trace['frame_file']).open('rb') as f:
        for line in f:
            assert len(line)<=1<<20 and line.endswith(b'\n')
            frame=json.loads(line)
            assert not offline and frame['connection']==trace['connections'][0]['id'] and frame['direction'] in streams
            data=base64.b64decode(frame['data'],validate=True);assert data
            streams[frame['direction']].feed(data);count+=1
    assert count==trace['frame_records']
    result={d:p.finish() for d,p in streams.items()}
    for d,p in result.items():assert p['bytes']==stats[d]
    if offline:
        assert count==0 and all(p['bytes']==0 for p in result.values())
    else:
        assert count>0 and result['client_to_server']['operations'].get('CONNECT')==1
        assert result['client_to_server']['api']
        legacy={subject:count for subject,count in result['client_to_server']['api'].items() if 'WF_JRN' in subject}
        purges=result['client_to_server']['legacy_purges']
        if legacy_purge_subject is None:
            assert not legacy and not purges
        else:
            assert legacy=={prefix+'STREAM.INFO.WF_JRN':1,prefix+'STREAM.PURGE.WF_JRN':1}
            assert purges==[{'filter':legacy_purge_subject}]
        infos=result['server_to_client']['infos'];assert infos
        assert all(p.get('domain','')==domain for p in infos)
        assert len({p['server_id'] for p in infos})==1
    return dict(domain=domain,offline=offline,legacy_purge_subject=legacy_purge_subject,frame_records=count,streams=result)
