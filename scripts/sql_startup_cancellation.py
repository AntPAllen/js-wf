"""Check the actual SQL lock boundary and startup child, read-only."""
from datetime import datetime
from pathlib import Path
import re
import sql_leaf_stream_wire


def validate(case,proof,revision,verify_log,verify_child,expected_signal='SIGTERM'):
    case=Path(case);boundary=proof['sql_startup_cancellation'];child=boundary['child']
    assert expected_signal in ('SIGTERM','SIGINT')
    signal_field='sigint_sent_at' if expected_signal=='SIGINT' else 'sigterm_sent_at'
    if expected_signal=='SIGINT':
        assert boundary['signal']=='SIGINT' and 'sigterm_sent_at' not in boundary
    else:
        assert boundary.get('signal','SIGTERM')=='SIGTERM' and 'sigint_sent_at' not in boundary
    verify_child(child,proof['standalone_binary'],revision)
    assert child['phase']=='sql-startup' and child['exit_code']==0 and 'wait_error' not in child and 'exit_signal' not in child
    assert child['pid'] not in {p['pid'] for p in proof['standalone_projectors']}
    backend=boundary['blocked_backend_pid'];blocker=boundary['blocker_backend_pid']
    assert type(backend) is int and type(blocker) is int and backend>0 and blocker>0 and backend!=blocker
    assert child['writer_backend_pid']==backend and child['startup_blocker_backend_pid']==blocker
    assert boundary['blocking_pid_confirmed'] is True and boundary['wait_event_type']=='Lock' and boundary['wait_event']=='relation'
    assert boundary['query'].startswith('CREATE INDEX IF NOT EXISTS wf_visibility_')
    assert all(type(boundary[k]) is int and boundary[k]==0 for k in ('residual_sessions','residual_locks','rows'))
    assert boundary['projection_durables_absent'] is True
    stamp=lambda x:datetime.fromisoformat(x.replace('Z','+00:00'))
    ordered=[boundary['lock_held_at'],child['admitted_at'],boundary['blocked_statement_admitted_at'],boundary[signal_field],child['reaped_at'],boundary['residual_checked_at'],boundary['blocker_released_at'],proof['standalone_projectors'][0]['admitted_at']]
    assert all(stamp(a)<=stamp(b) for a,b in zip(ordered,ordered[1:]))
    assert 0< (stamp(child['reaped_at'])-stamp(boundary[signal_field])).total_seconds()<10
    assert (case/'standalone-project-sql-startup.log').read_text()==''
    leaf=__import__('json').loads((case/'leaf-route/leaf-proof.json').read_text())
    root=case/'projector-sql-startup-leaf-wire'
    assert child['leaf_transport'] is True and child['nats_target']==leaf['leaf_url'] and child['nats_url']==child['argv'][2] and child['nats_url']!=child['nats_target'] and child['wire_root']==str(root)
    wire=sql_leaf_stream_wire.validate_phase(root,leaf['leaf_id'])
    assert wire['connection']['target']==leaf['leaf_url'].removeprefix('nats://')
    assert wire['encoded_bytes']+1024<=512<<20
    api=wire['streams']['client_to_server']['api']
    assert set(api)=={'$JS.WFVIEW.API.STREAM.INFO.WF_INV','$JS.WFVIEW.API.STREAM.INFO.WF_JRN'}
    assert all(n==1 for n in api.values()),'startup must stop in SQL Init before later NATS calls'
    expected=f"SQL startup cancelled: pid={child['pid']} backend={backend} blocker={blocker} wait=Lock/relation exit=0 residual_sessions=0 residual_locks=0 rows=0 durables_absent=true"
    if expected_signal=='SIGINT':expected+=' signal=SIGINT'
    assert verify_log.count(expected)==1
    assert ['SQL startup cancelled:'+line.split('SQL startup cancelled:',1)[1].strip('\r\n') for line in verify_log.splitlines() if 'SQL startup cancelled:' in line]==[expected]
    wireline=f"SQL projector leaf wire: phase=sql-startup pid={child['pid']} records={wire['frame_records']} client_bytes={wire['streams']['client_to_server']['bytes']} server_bytes={wire['streams']['server_to_client']['bytes']}"
    assert verify_log.count(wireline)==1
    return dict(boundary=boundary,wire=wire,signal=expected_signal,exit=0,scope='Real PostgreSQL relation lock holds packaged child in schema initialization until the selected signal and reap; full original50000 recovery follows.')
