#!/usr/bin/env python3
"""Corroborate each opt-in pending Sleep cut; never certify the full matrix."""
import base64
import json


def check(root, report, row, timestamp_ns):
    faults = json.loads((root/'faults.json').read_text())
    receipts = {r['sequence']: r for r in json.loads((root/'controller-receipts.json').read_text())}
    operations = json.loads((root/'controller-operations.json').read_text())
    bounds = json.loads((root/'controller-latency-audit.json').read_text())['bounds']
    paths = list(root.glob('fault-*-clock-timer-cut.json'))
    if not faults or any(f["node"] != 4 for f in faults) or len(paths) != len(faults) or len(faults) != report['confirmed_faults']:
        raise ValueError('incomplete pending timer cut proofs')
    offset = {'server_clock_ahead': 60_000_000_000, 'server_clock_behind': -60_000_000_000}[row]
    for index, fault in enumerate(faults, 1):
        cut = json.loads((root/f'fault-{index}-clock-timer-cut.json').read_text())
        admission = cut['admission']; request = admission['request']; suspended = admission['suspended']
        subject = 'wf.jrn.matrixtimer.' + admission['id']
        for receipt in (request, suspended):
            if receipt['subject'] != subject or receipts.get(receipt['sequence']) != receipt:
                raise ValueError('selected prefix differs from final independent receipts')
        if request['entry']['kind'] != 'StepRequested' or suspended['entry']['kind'] != 'Suspended' or request['sequence'] >= suspended['sequence']:
            raise ValueError('invalid selected timer prefix')
        payload = request['entry']['payload']; duration = payload['duration_nanos']
        if payload['kind'] != 'timer' or duration <= 0 or suspended['entry']['payload']['waiting_on'] != 'timer:' + payload['name']:
            raise ValueError('selected suspended tail is not the requested positive Sleep')
        tail = cut['refreshed_tail']
        if tail['Subject'] != subject or tail['Sequence'] != suspended['sequence'] or json.loads(base64.b64decode(tail['Data'], validate=True)) != suspended['entry']:
            raise ValueError('fresh retained tail differs from selected suspended receipt')
        origin = admission['origin']; end = timestamp_ns(origin['At']); start = end-origin['Duration']
        domain = payload.get('clock_domain', '')
        if origin not in operations or origin['Error'] or origin['Type'] != 'matrixtimer' or origin['ID'] != admission['id'] or origin['Worker'] != request['entry']['worker_id'] or origin['JournalIndex'] != request['entry']['index'] or origin['Duration'] < 0:
            raise ValueError('unproven timer clock origin')
        if admission['source_offset_ns'] != offset:
            raise ValueError('wrong selected source offset')
        earliest_due = start+duration
        if domain:
            if domain != 'utc-quorum-v1' or origin['Operation'] != 'timer_domain_clock' or origin.get('ClockDomain') != domain:
                raise ValueError('unknown canonical timer domain')
            lower, upper = timestamp_ns(origin['ClockLower']), timestamp_ns(origin['ClockUpper'])
            if lower > upper or lower > end or upper < start or timestamp_ns(payload['fire_at']) != upper+duration:
                raise ValueError('canonical deadline differs from its creation bounds')
            hint = admission['native_hint']
            if hint not in operations or hint['Operation'] != 'timer_native_hint' or hint.get('Error') or hint.get('TimerPublished') is not True or hint['Type'] != 'matrixtimer' or hint['ID'] != admission['id'] or hint['Worker'] != request['entry']['worker_id'] or hint['JournalIndex'] != request['entry']['index'] or hint['JournalKind'] != 'StepRequested' or hint['ClockDomain'] != domain or hint['Duration'] < 0:
                raise ValueError('unproven acknowledged native hint')
            hint_end = timestamp_ns(hint['At']); hint_start = hint_end-hint['Duration']
            physical = timestamp_ns(hint['ServerTime'])
            hint_lower, hint_upper = timestamp_ns(hint['ClockLower']), timestamp_ns(hint['ClockUpper'])
            if hint_end > timestamp_ns(suspended['observed_at']) or hint_lower > hint_upper or hint_lower > hint_end or hint_upper < hint_start or not hint_start+offset-2_000_000_000 <= physical <= hint_end+offset+2_000_000_000:
                raise ValueError('native hint did not use the shifted source')
            if timestamp_ns(hint['TimerDeadline']) != timestamp_ns(payload['fire_at']) or timestamp_ns(hint['TimerScheduleAt']) != physical+max(0,timestamp_ns(payload['fire_at'])-hint_lower):
                raise ValueError('native schedule translation changed')
            acknowledged = [op for op in operations if op.get('Operation') == 'timer_native_hint' and op.get('Type') == 'matrixtimer' and op.get('ID') == admission['id'] and op.get('Worker') == hint['Worker'] and op.get('JournalIndex') == hint['JournalIndex'] and op.get('TimerPublished') is True and not op.get('Error') and op.get('ClockDomain') == domain and op.get('TimerDeadline') == hint['TimerDeadline']]
            if len(acknowledged) != 1:
                raise ValueError('ambiguous acknowledged native hint')
        else:
            if origin['Operation'] != 'timer_clock' or origin['JournalKind'] != 'StepRequested':
                raise ValueError('unproven legacy timer clock origin')
            server = timestamp_ns(origin['ServerTime'])
            if not start+offset-2_000_000_000 <= server <= end+offset+2_000_000_000 or start-2_000_000_000 <= server <= end+2_000_000_000:
                raise ValueError('selected timer never used the shifted source')
            if timestamp_ns(payload['fire_at']) != server+duration:
                raise ValueError('selected deadline differs from its actual clock origin')
        if timestamp_ns(admission['earliest_due']) != earliest_due:
            raise ValueError('selected duration boundary changed')
        observed = timestamp_ns(admission['observed']); refreshed = timestamp_ns(cut['refreshed']); removed = timestamp_ns(cut['removed'])
        actual_ops = json.loads((root/f'fault-{index}-journal-operations.json').read_text())
        if removed != timestamp_ns(actual_ops[0]['at']) or not timestamp_ns(request['observed_at']) <= observed or not timestamp_ns(suspended['observed_at']) <= observed or not end <= observed <= refreshed <= timestamp_ns(fault['killed']) <= removed < earliest_due:
            raise ValueError('timer cut did not remove the source before its duration boundary')
        exit_proof = cut.get('exit_observation')
        if exit_proof is not None:
            if exit_proof['node'] != fault['node'] or not isinstance(exit_proof['container'], str) or not exit_proof['container'] or any(op.get('container') != exit_proof['container'] for op in actual_ops) or exit_proof['state'] not in ('exited', 'dead', 'absent'):
                raise ValueError('invalid Docker source exit proof')
            clock_path = root/'independent-clock.json'
            if clock_path.exists() and json.loads(clock_path.read_text())['config']['probes'][exit_proof['node']]['server'] != exit_proof['container']:
                raise ValueError('Docker exit proof names another clock source')
            started = timestamp_ns(exit_proof['kill_started'])
            returned = timestamp_ns(exit_proof['kill_returned'])
            stopped = timestamp_ns(exit_proof['source_stopped'])
            cleanup = timestamp_ns(exit_proof['cleanup_complete'])
            if exit_proof['state'] == 'absent' and stopped != cleanup:
                raise ValueError('absence must confirm exit and cleanup together')
            if not timestamp_ns(fault['killed']) <= started <= returned <= stopped == removed <= cleanup <= timestamp_ns(actual_ops[1]['at']):
                raise ValueError('Docker exit observation chronology changed')
        future = [b for b in bounds if b['type'] == 'matrixtimer' and b['id'] == admission['id'] and b['sequence'] > suspended['sequence']]
        if not future or any(timestamp_ns(b['before']) < removed for b in future):
            raise ValueError('final journal cannot corroborate the selected prefix at removal')
        selected = {b['sequence']: b for b in bounds if b['sequence'] in (request['sequence'], suspended['sequence'])}
        if len(selected) != 2 or any(selected[r['sequence']]['entry'] != r['entry'] for r in (request, suspended)):
            raise ValueError('selected prefix differs from final retained journal')
    return {'admitted_pending_sleep_cuts': len(faults), 'all_cuts_remove_source_before_duration': True,
            'corroborates_final_retained_prefix': True, 'admits_all_in_flight_timer_cut_combinations': False,
            'clears_full_tier3_release': False}
