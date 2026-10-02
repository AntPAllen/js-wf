#!/usr/bin/env python3
"""Check controller append windows and their conservative latency samples."""
from collections import defaultdict
import json
import math


def check(root, report, ns):
    read = lambda name: json.loads((root/name).read_text())
    proof = read('controller-latency-audit.json')
    if proof['clock'] != 'unshifted-controller-conservative-bounds' or proof['samples'] != read('latencies.json'):
        raise ValueError('controller latency provenance or retained samples disagree')
    operations = defaultdict(list)
    clock_operations = []
    for event in read('controller-operations.json'):
        if event['Operation'] in ('timer_clock', 'timer_domain_clock'):
            clock_operations.append(event)
            continue
        if event['Operation'] == 'timer_native_hint':
            # Native hint timing is independently checked by cut/role admission;
            # it cannot establish a canonical creation or completion deadline.
            continue
        if event['Operation'] != 'journal_append':
            raise ValueError('unexpected controller operation')
        key = tuple(event[k] for k in ('Type', 'ID', 'Worker', 'JournalIndex', 'JournalKind'))
        if event.get('Error', '') and 'journal append outcome unknown' not in event['Error']:
            continue
        if type(event['Duration']) is not int or event['Duration'] < 0:
            raise ValueError('invalid controller call duration')
        operations[key].append(event)
    receipts = {}
    for receipt in read('controller-receipts.json'):
        if receipt['sequence'] in receipts or not receipt['sequence']:
            raise ValueError('duplicate or invalid controller receipt')
        receipts[receipt['sequence']] = receipt
    bounds, identities = {}, defaultdict(list)
    for bound in proof['bounds']:
        seq = bound['sequence']
        if seq in bounds or not seq:
            raise ValueError('duplicate or invalid controller append window')
        key = tuple(bound[k] for k in ('type', 'id', 'worker', 'index', 'kind'))
        attempts = operations[key]
        if len(attempts) != bound['attempts'] or not attempts:
            raise ValueError('controller append window lacks its exact call attempts')
        lower = min(ns(e['At'])-e['Duration'] for e in attempts)
        uppers = [ns(e['At']) for e in attempts if not e.get('Error')]
        if seq in receipts:
            receipt = receipts[seq]
            if receipt['subject'] != f"wf.jrn.{bound['type']}.{bound['id']}" or receipt['entry'] != bound['entry']:
                raise ValueError('controller receipt identity differs from retained entry')
            uppers.append(ns(receipt['observed_at']))
        if not uppers or ns(bound['before']) != lower or ns(bound['after']) != min(uppers) or lower > min(uppers):
            raise ValueError('controller append bounds cannot be derived from calls/read receipts')
        if any(bound['entry'][field] != bound[target] for field, target in (('index', 'index'), ('kind', 'kind'), ('worker_id', 'worker'))):
            raise ValueError('controller append identity differs from its retained entry')
        bounds[seq] = bound
        identities[(bound['type'], bound['id'])].append(bound)
    if len(bounds) != report['journal_entries'] or len(identities) != report['invocations']:
        raise ValueError('controller timing evidence does not cover all audited entries/invocations')
    starts, signals = {}, {}
    for call in read('controller-client-calls.json'):
        identity = (call['args'].get('type'), call['args'].get('id'))
        at = ns(call['invoke_ts'])
        if call['op'] == 'start': starts[identity] = min(starts.get(identity, at), at)
        if call['op'] == 'signal' and call['result'].get('signal_seq'):
            seq = call['result']['signal_seq']; signals[seq] = min(signals.get(seq, at), at)
    for bound in bounds.values():
        payload = bound['entry'].get('payload') or {}
        if bound['kind'] == 'StepRequested' and payload.get('child_id'):
            identity = (payload['child_type'], payload['child_id'])
            at = ns(bound['before']); starts[identity] = min(starts.get(identity, at), at)
    timer_data = read('controller-timers.json')
    timer_deadlines, timer_latest = {}, {}
    for bound in bounds.values():
        payload = bound['entry'].get('payload') or {}
        if bound['kind'] != 'StepRequested' or payload.get('kind') != 'timer': continue
        domain = payload.get('clock_domain', '')
        if domain not in ('', 'utc-quorum-v1'):
            raise ValueError('unknown timer deadline domain')
        candidates = [e for e in clock_operations if
                   (e['Type'], e['ID'], e['Worker'], e['JournalIndex']) ==
                   (bound['type'], bound['id'], bound['worker'], bound['index']) and
                   not e.get('Error') and type(e['Duration']) is int and e['Duration'] >= 0]
        origins = []
        for event in candidates:
            if domain:
                if event['Operation'] != 'timer_domain_clock' or event.get('ClockDomain') != domain or not event.get('ClockLower') or not event.get('ClockUpper'):
                    continue
                lower, upper = ns(event['ClockLower']), ns(event['ClockUpper'])
                end, start = ns(event['At']), ns(event['At'])-event['Duration']
                if lower > upper or lower > end or upper < start or upper+payload['duration_nanos'] != ns(payload['fire_at']):
                    continue
            elif event['Operation'] != 'timer_clock' or not event.get('ServerTime') or ns(event['ServerTime'])+payload['duration_nanos'] != ns(payload['fire_at']):
                continue
            origins.append(event)
        if len(origins) != 1:
            raise ValueError('timer deadline lacks its unique successful creation clock lookup')
        origin = origins[0]; key = (bound['id'], payload['name'])
        timer_deadlines[key] = ns(origin['At'])-origin['Duration']+payload['duration_nanos']
        timer_latest[key] = ns(origin['At'])+payload['duration_nanos']
    samples = defaultdict(list)
    for sample in proof['samples']:
        key = (sample['type'], sample['id'])
        if key not in identities:
            raise ValueError('controller sample lacks a retained invocation')
        matches = [b for b in identities[key] if b['after'] == sample['observed'] and b['before'] == sample['observed_lower']]
        if not matches or sample['delay_ns'] != ns(sample['observed'])-ns(sample['enabled']) or sample['delay_ns'] < 0:
            raise ValueError('controller sample is not derived from its append window')
        if sample['event'] == 'terminal' and not any(b['kind'] == 'Completed' for b in matches):
            raise ValueError('terminal controller sample is not a completed append')
        enabled = ns(sample['enabled'])
        event = sample['event']
        if event == 'start' and enabled != starts.get(key):
            raise ValueError('controller start sample is not bound to the actual SDK/parent request')
        if event == 'signal_sent':
            candidates = [b for b in matches if b['kind'] == 'SignalConsumed']
            if not any(enabled == signals.get(b['entry']['payload']['sig_seq'], starts.get(key)) for b in candidates):
                raise ValueError('controller signal sample lacks its actual producer bound')
        if event == 'child_completed':
            children = [(b['entry']['payload']['child_type'], b['entry']['payload']['child_id'])
                        for b in identities[key] if b['kind'] == 'StepRequested' and (b['entry'].get('payload') or {}).get('child_id')]
            terminals = [b for child in children for b in identities.get(child, []) if b['kind'] == 'Completed']
            if not any(enabled == ns(b['before']) and any(m['sequence'] > b['sequence'] for m in matches) for b in terminals):
                raise ValueError('controller child sample lacks a causal terminal bound')
        if event == 'timer_due' and not any(enabled == value for (timer_id, _), value in timer_deadlines.items() if timer_id == sample['id']):
            raise ValueError('controller timer sample lacks an actual deadline bound')
        if event not in ('start', 'signal_sent', 'child_completed', 'timer_due', 'terminal'):
            raise ValueError('unknown controller enabling event')
        samples[sample['type']].append(sample)
    for identity in identities:
        own = [s for s in proof['samples'] if (s['type'], s['id']) == identity]
        if len([s for s in own if s['event'] == 'start']) != 1:
            raise ValueError('missing/duplicate controller start sample')
        terminals = [s for s in own if s['event'] == 'terminal']
        others = [ns(s['enabled']) for s in own if s['event'] != 'terminal']
        if len(terminals) != 1 or not others or ns(terminals[0]['enabled']) != max(others):
            raise ValueError('controller terminal sample lacks the final enabling bound')
    for typ, cell in report['cells'].items():
        terminal = [s for s in samples[typ] if s['event'] == 'terminal']
        if len(terminal) != cell['invocations'] or len({s['id'] for s in terminal}) != len(terminal):
            raise ValueError('missing/duplicate controller terminal sample')
        for label, values in (('terminal', terminal), ('progress', [s for s in samples[typ] if s['event'] != 'terminal'])):
            delays = sorted(s['delay_ns'] for s in values)
            if not delays or not math.isclose(delays[(99*len(delays)+99)//100-1]/1e9, cell[label+'_p99_seconds'], abs_tol=1e-9, rel_tol=0):
                raise ValueError('controller sample p99 differs from accepted cell')
    timers = timer_data
    if len(timers) != report['cells']['matrixtimer']['invocations']*8:
        raise ValueError('controller timer observations do not cover all eight waits')
    seen_timers = set()
    profile = report.get('clock_timer_cut_profile')
    if profile not in (None, 'first-wait-2s'):
        raise ValueError('unknown clock timer duration profile')
    for timer in timers:
        key = (timer['id'], timer['name'])
        expected_duration = 2_000_000_000 if profile == 'first-wait-2s' and timer['name'] == 'timer-0' else 250_000_000
        if key in seen_timers or timer['duration_ns'] != expected_duration:
            raise ValueError('duplicate or incorrect controller timer duration')
        seen_timers.add(key)
        due = timer_latest.get(key)
        if due is None: raise ValueError('timer call lacks its actual creation clock origin')
        if ns(timer['first_return']) < due:
            raise ValueError('controller timer returned before its minimum duration')
        entries = sorted(identities[('matrixtimer', timer['id'])], key=lambda b: b['index'])
        requests = [i for i, b in enumerate(entries) if b['kind'] == 'StepRequested' and b['entry']['payload'].get('name') == timer['name']]
        if len(requests) != 1:
            raise ValueError('controller timer has no unique retained request')
        completions = [b for b in entries[requests[0]+1:] if b['kind'] == 'StepCompleted']
        if entries[requests[0]]['entry']['payload'].get('duration_nanos') != timer['duration_ns'] or not completions or ns(completions[0]['before']) < due:
            raise ValueError('controller timer append can precede its minimum deadline')
    return dict(append_windows=len(bounds), invocations=len(identities), timer_waits=len(timers),
                clock=proof['clock'], uses_broker_timestamps=False, unknown_return_is_commit_upper_bound=False)
