"""Additional admission for the full copied-cohort cursor-owner fault proof."""
from datetime import datetime


def validate(result, servers, identity):
    def stamp(value):
        return datetime.fromisoformat(value.replace('Z', '+00:00'))

    assert result['cursor_owner_restart'] and result['parallel_decode']
    assert not result['qualifies_24h'] and result['cutoff'] == 238560
    assert result['error'] == '<nil>' and 0 < result['audit_ns'] <= result['elapsed_ns'] < 20_000_000_000
    assert result['report'] == dict(Invocations=238560, Journals=238560, Entries=2630779, Terminal=238560)
    state = result['readiness']['WF_JRN']['state']
    assert state['first_seq'] == 1 and state['last_seq'] == state['messages'] == result['journal_visits'] == 2632932
    assert not state.get('num_deleted', 0)
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
    return dict(actual_cursor_owner=node, raw_journal_visits=result['journal_visits'],
                resumed_start_sequence=resumed['info']['config']['opt_start_seq'],
                target_cursor=target['name'], replacement_cursor=resumed['info']['name'],
                actual_servers=6, same_store_restart=True, old_cursor_deletion_error=deletions[target['name']]['error'])
