import copy
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('workers_review', Path(__file__).with_name('review-tier2-retained-workers.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


class RetainedWorkerFaultReview(unittest.TestCase):
    def fixture(self, row):
        base = datetime(2026, 10, 6, tzinfo=timezone.utc)
        stamp = lambda second: (base+timedelta(seconds=second)).isoformat().replace('+00:00', 'Z')
        faults, records = [], []
        worker = 'matrix-process-0-generation-0'
        for index in range(10):
            at = index*60
            fault = dict(scheduled=stamp(at), killed=stamp(at), resumed=stamp(at+45),
                         healed=stamp(at+46), worker=worker, pid=7, active_leases=1, fencing_events=1)
            event = dict(At=stamp(at+45.5), Worker=worker, Type='short', ID=f'x-{index}',
                         RunSequence=index+1, Delivery=1, Error='workflow lease was lost')
            if row == 'pause':
                fault.update(paused=stamp(at), paused_leases=[dict(key=f'short.x-{index}',
                             worker_id=worker, epoch=index+1, revision=index+1)])
                event['Epoch'] = index+1
                records.append(dict(pid=7, sequence=index+1, event=event))
            else:
                stats = dict(responses_held=False, held_bytes=0, client_to_server=0,
                             server_to_client=0, buffer_overflows=0)
                fault.update(worker_ping_at=stamp(at+45.5), proxy_before=stats,
                             proxy_blocked=dict(stats, responses_held=True, held_bytes=1, client_to_server=1),
                             proxy_healed=dict(stats, server_to_client=1),
                             isolation_target=dict(token=str(index), delivery=dict(event, Stage='lease_acquired')))
                records.append(event)
            faults.append(fault)
        return faults, {7: dict(worker_id=worker)}, records

    def check(self, row, faults, workers, records):
        return r.review_faults(row, faults, workers, lambda worker, kind: records)

    def test_accepts_both_full_recorded_fault_patterns(self):
        for row in ('pause', 'isolation'):
            self.assertEqual(self.check(row, *self.fixture(row)), 10)

    def test_rejects_wrong_ownership_timing_count_and_schedule(self):
        for row in ('pause', 'isolation'):
            for mutation in ('short', 'count', 'pid', 'worker', 'late_fence', 'early_fence', 'cadence', 'error', 'bool_active', 'bool_fence'):
                with self.subTest(row=row, mutation=mutation):
                    faults, workers, records = self.fixture(row)
                    if mutation == 'short': faults[0]['resumed'] = '2026-10-06T00:00:44.999999999Z'
                    elif mutation == 'count': faults.pop()
                    elif mutation == 'pid': faults[0]['pid'] = 8
                    elif mutation == 'worker': faults[0]['worker'] = 'wrong'
                    elif mutation == 'late_fence':
                        event = records[0]['event'] if row == 'pause' else records[0]
                        event['At'] = '2026-10-06T00:00:46.000000001Z'
                    elif mutation == 'early_fence':
                        event = records[0]['event'] if row == 'pause' else records[0]
                        event['At'] = '2026-10-05T23:59:59.999999999Z'
                    elif mutation == 'cadence': faults[1]['scheduled'] = faults[0]['scheduled']
                    elif mutation == 'error': faults[0]['error'] = 'unconfirmed'
                    elif mutation == 'bool_active': faults[0]['active_leases'] = True
                    elif mutation == 'bool_fence': faults[0]['fencing_events'] = True
                    with self.assertRaises(ValueError): self.check(row, faults, workers, records)

    def test_pause_rejects_wrong_epoch_and_record_sequence(self):
        for mutation in ('epoch', 'sequence', 'record_pid', 'lease_worker'):
            faults, workers, records = self.fixture('pause')
            if mutation == 'epoch': records[0]['event']['Epoch'] = 99
            elif mutation == 'sequence': records[0]['sequence'] = 99
            elif mutation == 'record_pid': records[0]['pid'] = 99
            else: faults[0]['paused_leases'][0]['worker_id'] = 'wrong'
            with self.assertRaises(ValueError): self.check('pause', faults, workers, records)

    def test_isolation_requires_exact_delivery_transport_and_fresh_ping(self):
        for mutation in ('run', 'delivery', 'type', 'id', 'not_fenced', 'held', 'requests', 'replies', 'overflow', 'ping', 'stage'):
            with self.subTest(mutation=mutation):
                faults, workers, records = self.fixture('isolation')
                if mutation in ('run', 'delivery', 'type', 'id'):
                    key = dict(run='RunSequence', delivery='Delivery', type='Type', id='ID')[mutation]
                    records[0][key] = 'wrong'
                elif mutation == 'not_fenced': records[0]['Error'] = 'nats: timeout'
                elif mutation == 'held': faults[0]['proxy_blocked']['held_bytes'] = 0
                elif mutation == 'requests': faults[0]['proxy_blocked']['client_to_server'] = 0
                elif mutation == 'replies': faults[0]['proxy_healed']['server_to_client'] = 0
                elif mutation == 'overflow': faults[0]['proxy_healed']['buffer_overflows'] = 1
                elif mutation == 'ping': faults[0]['worker_ping_at'] = faults[0]['resumed']
                elif mutation == 'stage': faults[0]['isolation_target']['delivery']['Stage'] = 'fetched'
                with self.assertRaises(ValueError): self.check('isolation', faults, workers, records)
