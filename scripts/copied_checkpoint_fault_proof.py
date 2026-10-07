"""Additional admission for the full copied-cohort cursor-owner fault proof."""
from datetime import datetime


def validate(result, servers, identity, *, checkpoint=8520):
    assert type(checkpoint) is int and checkpoint in (8520,4160)
    cutoff=238560 if checkpoint==8520 else 116480
    def stamp(value):
        return datetime.fromisoformat(value.replace('Z', '+00:00'))

    assert result['cursor_owner_restart'] and result['parallel_decode']
    assert not result['qualifies_24h'] and result['cutoff'] == cutoff
    assert result['error'] == '<nil>' and 0 < result['audit_ns'] <= result['elapsed_ns'] < 20_000_000_000
    entries=result['report']['Entries']
    assert type(entries) is int and entries>0
    if checkpoint==8520: assert entries==2630779
    assert result['report'] == dict(Invocations=cutoff, Journals=cutoff, Entries=entries, Terminal=cutoff)
    state = result['readiness']['WF_JRN']['state']
    assert state['first_seq'] == 1 and state['last_seq'] == state['messages'] == result['journal_visits']
    if checkpoint==8520: assert result['journal_visits']==2632932
    assert type(result['journal_visits']) is int and result['journal_visits']>=entries>=cutoff
    assert not state.get('num_deleted', 0)
    if checkpoint==4160:
        before=result['readiness_before_cleanup']
        inherited=result['inherited_cursors'];deletions=result['inherited_deletions']
        assert len(inherited)==len(deletions)==2
        assert {c['stream_name'] for c in inherited}=={'WF_INV','WF_JRN'}
        names={c['name'] for c in inherited};assert len(names)==2
        assert {d['name'] for d in deletions}==names and all(not d['error'] for d in deletions)
        finished=stamp(result['inherited_cleanup_finished'])
        for c in inherited:
            assert c['name'].startswith('wf-audit-') and c['config']['num_replicas']==1
            assert c['config']['mem_storage'] and c['config']['ack_policy']=='none'
            assert stamp(c['created'])<stamp('2026-10-07T18:01:14Z')
            source=c['stream_name'];old=before[source];new=result['readiness'][source]
            assert old['config']==new['config'] and old['state']['consumer_count']==1
            state=dict(old['state']);state['consumer_count']=0
            assert state==new['state']
        assert all(stamp(d['at'])<=finished for d in deletions)
        trial=[c['info'] for c in result['cursors'] if c['created']]
        assert all(stamp(c['created'])>=finished and c['name'] not in names for c in trial)
        for frame in (result.get('state_connection_loss') or {}).get('frames',[]):
            assert finished.timestamp()<=stamp(frame['deadline']).timestamp()-20
    assert result['cleanup'] == dict(WF_INV=0, WF_JRN=0)
    assert set(result['readiness_after']) == {'WF_INV', 'WF_JRN', 'KV_WF_STATE', 'WF_PURGE'}
    for info in result['readiness_after'].values():
        assert info['config']['num_replicas'] == 5 and info['config']['storage'] == 'file'
        assert info['cluster']['leader'] and len(info['cluster']['replicas']) == 4
        assert all(p['current'] and not p.get('offline', False) for p in info['cluster']['replicas'])
    watches = result['state_watches']
    assert 1 <= len(watches) <= 3
    previous_stop = 0
    for watch in watches:
        assert previous_stop <= watch['StartedNS'] <= watch['CreatedNS'] <= result['elapsed_ns']
        if watch['CreationError'] == '<nil>':
            assert watch['CreatedNS'] <= watch['StopStartedNS'] <= watch['StopFinishedNS'] <= result['elapsed_ns']
            assert watch['StopFinishedNS'] > 0
            previous_stop = watch['StopFinishedNS']
        else:
            assert watch['StopStartedNS'] == watch['StopFinishedNS'] == 0 and not watch['StopError']
            previous_stop = watch['CreatedNS']
    assert watches[-1]['CreationError'] == watches[-1]['StopError'] == '<nil>'
    target, kill = result['target'], result['kill']
    assert target['stream_name'] == 'WF_JRN' and target['num_pending'] > 0
    config = target['config']
    assert config['num_replicas'] == 1 and config['mem_storage'] and config['ack_policy'] == 'none'
    node = kill['node']
    assert node in range(5) and target['cluster']['leader'] == identity + '-n' + str(node)
    assert kill['concurrent_observation'] and kill['state'] in ('exited', 'dead', 'absent')
    assert stamp(kill['kill_started']) <= stamp(kill['source_stopped']) <= stamp(result['restart_completed'])
    assert stamp(kill['kill_started']) <= stamp(kill['kill_returned']) <= stamp(result['restart_completed'])
    created = [c for c in result['cursors'] if c['created']]
    journals = [c for c in created if c['info']['stream_name'] == 'WF_JRN']
    invocations = [c for c in created if c['info']['stream_name'] == 'WF_INV']
    assert len(created) == 3 and len(journals) == 2 and len(invocations) == 1
    first, resumed = journals
    assert first['info']['name'] == target['name'] != resumed['info']['name']
    assert first['journal_visits'] == 0 and first['info']['config']['opt_start_seq'] == 1
    assert 128 <= resumed['journal_visits'] < result['journal_visits']
    assert resumed['info']['config']['opt_start_seq'] == resumed['journal_visits'] + 1
    for cursor in created:
        config = cursor['info']['config']
        assert config['num_replicas'] == 1 and config['mem_storage'] and config['ack_policy'] == 'none'
    deletions = {d['name']: d for d in result['deletions']}
    assert len(deletions) == len(result['deletions']) == 3
    assert set(deletions) == {c['info']['name'] for c in created}
    assert not deletions[resumed['info']['name']]['error']
    assert not deletions[invocations[0]['info']['name']]['error']
    assert deletions[target['name']]['error'] in ('', 'nats: consumer not found')
    names = [s['args'][s['args'].index('-n') + 1] for s in servers]
    assert len(servers) == 6 and set(names) == {identity + '-n' + str(n) for n in range(5)}
    owner = [s for s, name in zip(servers, names) if name == identity + '-n' + str(node)]
    assert len(owner) == 2
    before, after = sorted(owner, key=lambda s: s['observed_utc'])
    assert before['container']['Name'].lstrip('/') == after['container']['Name'].lstrip('/') == kill['container']
    assert before['container']['Id'] != after['container']['Id'] and before['pid'] != after['pid']
    assert stamp(before['observed_utc']) <= stamp(kill['kill_started']) <= stamp(after['observed_utc'])
    before_mount = [m for m in before['container']['Mounts'] if m['Destination'] == '/data']
    after_mount = [m for m in after['container']['Mounts'] if m['Destination'] == '/data']
    assert len(before_mount) == len(after_mount) == 1 and before_mount[0]['Source'] == after_mount[0]['Source']
    connection = result.get('state_connection_loss')
    connection_review = None
    if connection is not None:
        assert connection['owner'] == node and connection['server_name'] == identity + '-n' + str(node)
        assert connection['server_id'] and connection['status_after_close'] == 'CLOSED'
        ports = before['container']['NetworkSettings']['Ports']['4222/tcp']
        assert len(ports) == 1 and connection['url'] == 'nats://127.0.0.1:' + ports[0]['HostPort']
        assert stamp(connection['native_created']) <= stamp(connection['close_started']) <= stamp(connection['close_finished'])
        peer_outage = connection.get('peer_outage', False)
        if peer_outage:
            assert connection['exposed_status'] == 'RECONNECTING' and connection['disconnect_error']
            assert stamp(connection['native_created']) <= stamp(kill['kill_started']) <= stamp(connection['disconnected']) <= stamp(connection['exposed'])
            assert stamp(kill['source_stopped']) <= stamp(connection['exposed']) <= stamp(connection['close_started'])
            assert stamp(connection['restart_started']) <= stamp(result['restart_completed'])
            assert stamp(connection['exposed']) <= stamp(connection['close_started']) <= stamp(connection['close_finished'])
            hold = (stamp(connection['restart_started']) - stamp(kill['source_stopped'])).total_seconds()
            assert hold >= 3 and connection['offline_hold_ns'] >= 3_000_000_000
            assert abs(hold - connection['offline_hold_ns']/1e9) < .00001
        else:
            assert stamp(connection['close_finished']) <= stamp(connection['exposed'])
            assert stamp(connection['close_finished']) <= stamp(kill['kill_started'])
        assert 0 <= connection['buffered_before_exposure'] < cutoff
        assert connection['attempts'] == len(watches) and 2 <= len(watches) <= 3
        assert watches[0]['StopError'] in (('<nil>', 'nats: consumer not found', 'nats: connection closed', 'nats: invalid subscription') if peer_outage else ('nats: connection closed', 'nats: invalid subscription'))
        frames = connection['frames']
        assert frames and len({f['deadline'] for f in frames}) == 1
        returns = [f for f in frames if f['event'] == 'attempt_return']
        assert len(returns) == len(watches)
        first_frame, final = returns[0], returns[-1]
        assert not first_frame['initial_complete'] and first_frame['error']
        assert 'closed before initial completion' in first_frame['error'] or 'made no progress' in first_frame['error']
        assert first_frame['received'] < cutoff
        if peer_outage:
            assert 'made no progress' in first_frame['error'] and first_frame['elapsed_ns'] >= 2_000_000_000
            decisions = [f for f in frames if f['event'] == 'watch_idle_timeout']
            assert len(decisions) == 1
            decision = decisions[0]
            assert 'made no progress' in decision['error'] and not decision['initial_complete']
            assert decision['received'] == first_frame['received'] and decision['included'] == first_frame['included']
            created_frames = [f for f in frames if f['event'] == 'watch_created']
            assert created_frames and (stamp(decision['time']) - stamp(created_frames[0]['time'])).total_seconds() >= 2
            assert stamp(connection['exposed']) < stamp(decision['time']) <= stamp(connection['close_started'])
            assert stamp(decision['time']) < stamp(connection['restart_started'])
            assert stamp(connection['close_finished']) <= stamp(first_frame['time'])
        assert final['initial_complete'] and not final.get('error')
        assert final['received'] == result['readiness']['KV_WF_STATE']['state']['messages']
        assert final['included'] == result['cutoff']
        assert all(not f['initial_complete'] and f.get('error') for f in returns[:-1])
        barriers = [f for f in frames if f['event'] == 'initial_complete']
        assert len(barriers) == 1 and barriers[0]['received'] == final['received'] and barriers[0]['included'] == final['included']
        assert stamp(barriers[0]['time']) >= stamp(result['restart_completed'])
        for i, frame in enumerate(returns):
            assert frame['received'] >= frame['included'] >= 0
            if i > 0:
                assert stamp(frame['time']) > stamp(returns[i-1]['time'])
        connection_review = dict(controlled_real_connection_close=not peer_outage, actual_direct_peer_outage=peer_outage, actual_connected_server_id=connection['server_id'],
            watch_attempts=len(watches), abandoned_received=first_frame['received'], fresh_received=final['received'],
            original_deadline=final['deadline'], natural_outage_or_historical_cause=False)
    return dict(actual_cursor_owner=node, raw_journal_visits=result['journal_visits'],
                resumed_start_sequence=resumed['info']['config']['opt_start_seq'],
                target_cursor=target['name'], replacement_cursor=resumed['info']['name'],
                actual_servers=6, same_store_restart=True, connection_review=connection_review, old_cursor_deletion_error=deletions[target['name']]['error'])
