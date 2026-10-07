"""Review a real stock-leaf SIGKILL while packaged SQL catch-up is active."""
from datetime import datetime
import hashlib,json
from pathlib import Path
import sql_leaf_stream_wire


def validate(case,proof,revision,log,verify_child):
    case=Path(case);fault=proof['leaf_transport_fault'];child=fault['child']
    verify_child(child,proof['standalone_binary'],revision)
    assert child['phase']=='leaf-fault' and child['exit_code']==1 and child['wait_error'] and 'exit_signal' not in child
    assert child['pid'] not in {p['pid'] for p in proof['standalone_projectors']}|{proof['sql_startup_cancellation']['child']['pid']}
    assert type(fault['baseline_rows']) is int and type(fault['admitted_rows']) is int and 0<fault['baseline_rows']<fault['admitted_rows']<50000
    assert type(fault['sql_sessions_after_reap']) is int and fault['sql_sessions_after_reap']==0 and fault['sql_healthy_after_reap'] is True and fault['sql_backend_terminated'] is False
    leaf=json.loads((case/'leaf-route/leaf-proof.json').read_text());old=json.loads((case/'leaf-route/leaf.process.json').read_text());new=json.loads((case/'leaf-route/leaf.replacement.process.json').read_text())
    stamp=lambda x:datetime.fromisoformat(x.replace('Z','+00:00'))
    order=[proof['standalone_projectors'][1]['reaped_at'],child['admitted_at'],fault['fault_started'],old['kill_started_at'],old['reaped_at'],child['reaped_at'],fault['sql_checked_at'],new['admitted_at'],leaf['leaf_replacement_ready_at'],fault['healed_at'],proof['standalone_projectors'][2]['admitted_at']]
    assert all(stamp(a)<=stamp(b) for a,b in zip(order,order[1:]))
    assert old['exit_signal']=='killed' and old['exit_code']==-1 and old['reaped'] is True and new['exit_code']==0 and new['reaped'] is True
    assert 0<(stamp(child['reaped_at'])-stamp(fault['fault_started'])).total_seconds()<15
    assert 0<(stamp(fault['healed_at'])-stamp(fault['fault_started'])).total_seconds()<30
    assert (case/'standalone-project-leaf-fault.log').read_text().strip()
    root=case/'projector-leaf-fault-leaf-wire'
    assert child['leaf_transport'] is True and child['nats_target']==leaf['leaf_url'] and child['nats_url']==child['argv'][2] and child['nats_url']!=child['nats_target'] and child['wire_root']==str(root)
    capture=sql_leaf_stream_wire.validate_phase(root,leaf['leaf_id'],allow_fault=True)
    assert capture['connection']['target']==leaf['leaf_url'].removeprefix('nats://') and capture['encoded_bytes']+1024<=512<<20
    assert stamp(capture['connection']['at'])<stamp(fault['fault_started'])
    # Fault-only byte tails are preserved/hash-accounted, never promoted to
    # complete API packets. The three original healthy traces remain strict.
    assert log.count(f"SQL leaf SIGKILL: pid={old['pid']} id={leaf['leaf_id']} reaped=true")==1
    assert log.count(f"SQL leaf replacement: old_pid={old['pid']} pid={new['pid']} old_id={leaf['leaf_id']} id={leaf['leaf_replacement_id']} same_ports_store=true")==1
    assert log.count(f"SQL leaf transport fault: pid={child['pid']} backend={child['writer_backend_pid']} baseline_rows={fault['baseline_rows']} admitted_rows={fault['admitted_rows']} exit=1 SQL_healthy=true sessions=0")==1
    assert log.count(f"SQL projector leaf wire: phase=leaf-fault pid={child['pid']} records={capture['frame_records']} client_bytes={capture['streams']['client_to_server']['bytes']} server_bytes={capture['streams']['server_to_client']['bytes']}")==1
    return dict(fault=fault,wire=capture,original_leaf=old,replacement_leaf=new,scope='Full original50000 projection after stock leaf SIGKILL during actual partial SQL catchup; SQL remains healthy, fatal child reaped before same-leaf-store/ports restart and final replacement. All fault-phase bytes retained; interrupted packet tails separately hashed. No naturally lost reply, follower lag, NATS hub process kill or fullmatrix claim.')
