"""Validate the recorded remote-domain leaf retirement scenario, not full release."""
from datetime import datetime

def validate(proof, expected_profile="hub-restart"):
    assert expected_profile in ('hub-restart','leaf-sigkill-hub-restart')
    assert proof.get('fault_profile','hub-restart') == expected_profile
    process_leaf=expected_profile=='leaf-sigkill-hub-restart'
    assert proof['scenario_passed'] is True
    assert proof['local_domain']=='WFEDGE' and proof['remote_domain']=='WFRETIRE'
    assert proof['local_streams_before']==proof['local_streams_after']==0
    assert proof['leaf_disconnected'] is True and proof['leaf_reconnected'] is True
    assert len(proof['runtime_server_ids'])==3 and set(proof['runtime_server_ids'])=={proof['leaf_id']}
    before,after=proof['before'],proof['after']
    assert len(before)==len(after)==3
    names={f'wf-test-{i}' for i in range(3)}
    assert {x['name'] for x in before}=={x['name'] for x in after}==names
    assert all(x['domain']=='WFRETIRE' for x in before+after)
    old,new={x['id'] for x in before},{x['id'] for x in after}
    assert len(old)==len(new)==3 and old.isdisjoint(new)
    assert len(proof['stopped'])==3 and set(proof['stopped'])==old
    assert proof['leaf_id'] and proof['leaf_id'] not in old|new
    if process_leaf:
        assert proof['leaf_exit_observed'] is True and proof['leaf_signal']=='killed'
        assert type(proof['leaf_pid_before']) is int and type(proof['leaf_pid_after']) is int
        assert proof['leaf_pid_before']>0 and proof['leaf_pid_after']>0 and proof['leaf_pid_before']!=proof['leaf_pid_after']
        assert proof['client_disconnects']==[True,True,True]
        assert proof['leaf_original_id'] and proof['leaf_original_id'] not in old|new|{proof['leaf_id']}
    for key in ('leaf_before','leaf_after'):
        row=proof[key]
        assert row['server_id']==(proof['leaf_original_id'] if process_leaf and key=='leaf_before' else proof['leaf_id']) and row['leafnodes']==len(row['leafs'])==1
        assert row['leafs'][0]['name'] in names and row['leafs'][0]['account']=='$G'
    start=datetime.fromisoformat(proof['cut_start'].replace('Z','+00:00'))
    end=datetime.fromisoformat(proof['cut_end'].replace('Z','+00:00'))
    assert 0<(end-start).total_seconds()<30
    api={name:count for name,count in proof['subjects'].items() if '.API.' in name}
    assert api and all(name.startswith('$JS.WFRETIRE.API.') and type(count) is int and count>0 for name,count in api.items())
    assert any('.DIRECT.GET.OBJ_WF_BLOB.' in name for name in api)
    assert any('.CONSUMER.CREATE.OBJ_WF_BLOB.' in name for name in api)
    assert any('STREAM.MSG.GET.KV_WF_STATE' in name for name in api)
    return dict(remote_domain='WFRETIRE',local_domain='WFEDGE',hub_originals=3,hub_replacements=3,
                fault_profile=expected_profile,leaf_process_sigkill=process_leaf,leaf_disconnect_and_reconnect=True,whole_cut_seconds=(end-start).total_seconds(),
                api_subjects=len(api),scope='Single leaf-connected strict retirement/reuse and graceful all-hub restart, plus actual leaf SIGKILL only when explicitly selected; no all-hub SIGKILL, lease-expiry, daemon-child wire or broad leaf matrix claim.')
