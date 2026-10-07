"""Admit real pre-publication creation recovery on the complete copied R5 cohort."""
import base64
import importlib.util
import json
from pathlib import Path
import re

from worker_leaf_wire import protocol

spec=importlib.util.spec_from_file_location('copied_creation_clock',Path(__file__).with_name('check-tier2-journal-shard.py'))
clock=importlib.util.module_from_spec(spec);spec.loader.exec_module(clock)
ns=clock.timestamp_ns


def validate(root,result,servers,identity,*,require_fresh_metadata=True):
    root=Path(root);p=result['state_creation_stall']
    assert p['creation_control_valid'] is True
    assert ns(p['metadata_deadline'])<ns(p['deadline'])
    if require_fresh_metadata:
        metadata=p['metadata_lookup']
        assert metadata['deadline']==p['metadata_deadline'] and metadata['error']=='<nil>'
        assert ns(p['deadline'])-20_000_000_000<=ns(metadata['started'])<=ns(metadata['returned'])<=ns(p['first_creation_started'])
        assert ns(metadata['returned'])<ns(metadata['deadline'])
    assert result['cutoff']==116480 and not result.get('state_connection_loss')
    assert type(p['attempts']) is int and 2<=p['attempts']<=3
    assert type(p['parent_budget_ns']) is int and p['parent_budget_ns']==20_000_000_000
    elapsed=p['first_creation_elapsed_ns']
    assert type(elapsed) is int and 2_000_000_000<=elapsed<3_000_000_000
    started,returned,joined=[ns(p[k]) for k in ('first_creation_started','first_creation_returned','transport_joined')]
    assert 2_000_000_000<=returned-started<3_000_000_000 and abs(returned-started-elapsed)<1_000_000
    assert returned<=joined<=ns(result['kill']['kill_started'])
    assert p['first_native_error']=='context canceled'
    assert re.fullmatch('N[A-Z2-7]{55}',p['server_id']) and p['server_name']==identity+'-n0'
    peers=[s for s in servers if s['args'][s['args'].index('-n')+1]==p['server_name']]
    assert peers
    first=min(peers,key=lambda s:s['observed_utc'])
    ports=first['container']['NetworkSettings']['Ports']['4222/tcp'];assert len(ports)==1
    assert p['upstream_url']=='nats://127.0.0.1:'+ports[0]['HostPort']
    before,after=p['pending_before_close'],p['pending_after_close']
    for pending,disposition in [(before,'held'),(after,'cancelled')]:
        assert type(pending['connection']) is int and pending['connection']==1
        assert type(pending['forwarded_bytes']) is int and pending['forwarded_bytes']==0
        assert pending['disposition']==disposition
    assert {k:v for k,v in before.items() if k!='disposition'}=={k:v for k,v in after.items() if k!='disposition'}
    held=protocol(base64.b64decode(before['packet'],validate=True));assert len(held)==1
    op,fields,data=held[0];assert op in (b'PUB',b'HPUB') and fields[1].decode()==before['subject']
    assert before['subject'].startswith('$JS.API.CONSUMER.CREATE.KV_WF_STATE.')
    request=json.loads(data);assert request['stream_name']=='KV_WF_STATE'
    config=request['config']
    assert config['deliver_policy']=='last_per_subject' and config['ack_policy']=='none' and config['filter_subject']=='$KV.WF_STATE.>'
    assert type(config['num_replicas']) is int and config['num_replicas']==1 and config['mem_storage'] is True
    trace,stats=p['proxy_trace'],p['proxy_stats']
    assert trace['truncated'] is False and not trace.get('frame_file_error') and trace['frame_file']=='creation-wire.jsonl'
    assert trace['frames'] is None and len(trace['connections'])==1
    connection=trace['connections'][0]
    assert connection['id']==1 and connection['target']==p['upstream_url'].removeprefix('nats://')
    for key in ('upstream_dial_failures','held_bytes','active_connections','buffered_bytes','buffer_overflows'):
        assert type(stats[key]) is int and stats[key]==0
    assert type(stats['accepted_connections']) is int and stats['accepted_connections']==1 and stats['responses_held'] is False
    streams={k:bytearray() for k in ('client_to_server','server_to_client')};count=0
    for line in (root/'creation-wire.jsonl').read_text().splitlines():
        frame=json.loads(line);assert type(frame['connection']) is int and frame['connection']==1
        assert frame['direction'] in streams and ns(frame['at'])<=joined
        streams[frame['direction']].extend(base64.b64decode(frame['data'],validate=True));count+=1
    assert type(trace['frame_records']) is int and trace['frame_records']==count>0
    for direction,data in streams.items():
        assert type(stats[direction]) is int and stats[direction]==len(data)>0
    outgoing,incoming=protocol(streams['client_to_server']),protocol(streams['server_to_client'],True)
    assert sum(op==b'CONNECT' for op,_,_ in outgoing)==1 and incoming[0][0]==b'INFO'
    assert all(info['server_id']==p['server_id'] and info['server_name']==p['server_name'] for op,_,info in incoming if op==b'INFO')
    publications=[fields[1].decode() for op,fields,_ in outgoing if op in (b'PUB',b'HPUB')]
    assert '$JS.API.STREAM.INFO.KV_WF_STATE' in publications
    assert not any(subject.startswith('$JS.API.CONSUMER.CREATE.KV_WF_STATE.') for subject in publications)
    metadata=[]
    for op,_,payload in incoming:
        if op!=b'MSG':continue
        try:record=json.loads(payload)
        except (ValueError,TypeError):continue
        if record.get('config',{}).get('name')=='KV_WF_STATE':metadata.append(record)
    assert metadata
    for record in metadata:
        assert record['config']['num_replicas']==5 and record['config']['storage']=='file'
        assert all(record['state'][key]==result['readiness']['KV_WF_STATE']['state'][key] for key in ('messages','first_seq','last_seq'))
    frames=p['frames'];assert frames and all(f['deadline']==p['deadline'] for f in frames)
    times=[ns(f['time']) for f in frames];assert times==sorted(times) and times[-1]<ns(p['deadline'])
    assert started>=times[0] and ns(result['inherited_cleanup_finished'])<=ns(p['deadline'])-20_000_000_000
    assert [f['event'] for f in frames[:3]]==['watch_start','watch_creation_error','attempt_return']
    assert joined<=times[1] and all(f['received']==f['included']==f['last_revision']==0 and f['initial_complete'] is False for f in frames[:3])
    assert 'creation made no progress: nats: timeout' in frames[1]['error'] and 'initial_complete=false' in frames[2]['error']
    returns=[f for f in frames if f['event']=='attempt_return'];assert len(returns)==p['attempts']==len(result['state_watches'])
    assert all(not f['initial_complete'] and f.get('error') for f in returns[:-1])
    final=returns[-1];assert final['initial_complete'] is True and not final.get('error')
    assert final['received']==result['readiness']['KV_WF_STATE']['state']['messages'] and final['included']==116480
    barriers=[f for f in frames if f['event']=='initial_complete'];assert len(barriers)==1
    assert all(barriers[0][k]==final[k] for k in ('received','included','last_revision'))
    assert result['readiness_after']['KV_WF_STATE']['state']['consumer_count']==0
    return dict(first_creation_elapsed_ns=elapsed,attempts=p['attempts'],wire_frames=count,client_bytes=len(streams['client_to_server']),server_bytes=len(streams['server_to_client']),
                actual_upstream_server_id=p['server_id'],full_included=116480,native_received=final['received'],held_publication_bytes=0,transport_joined_before_cursor_kill=True,
                fresh_metadata_lookup_required=require_fresh_metadata,scope='Controlled R5 full copied cohort creation stall and cold cursor-owner recovery; no natural stall/cause, concurrent24h, physical store decoder or default release qualification.')
