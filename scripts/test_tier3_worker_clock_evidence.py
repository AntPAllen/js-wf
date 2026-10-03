import base64
import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('clock_evidence', Path(__file__).with_name('tier3-worker-clock-evidence.py'))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)


class WorkerClockEvidence(unittest.TestCase):
    def fixture(self):
        at = '2026-10-03T12:00:00.123456789Z'
        samples, messages = [], []
        for sequence, (worker, offset) in enumerate(c.OFFSETS.items(), 1):
            worker_at = c.corrected_timestamp(at, -offset)
            samples.append(dict(worker=worker, worker_at=worker_at, server_at=at,
                                sequence=sequence, offset_ns=offset))
            payload = dict(worker=worker, worker_at=worker_at, sequence=0, offset_ns=0)
            messages.append(dict(subject='matrix.clock.' + worker, sequence=sequence,
                                 time=at, data=base64.b64encode(json.dumps(payload).encode()).decode()))
        return dict(observed=at, samples=samples, messages=messages)

    def test_all_five_broker_clock_messages_and_offsets(self):
        result = c.check_probe(self.fixture())
        self.assertEqual(result['broker_clock_messages'], 5)
        self.assertEqual(result['deliberate_offsets_ns'], c.OFFSETS)
        self.assertFalse(result['proves_full_row'])

    def test_rejects_missing_duplicate_unshifted_stale_and_invented_samples(self):
        for mutation in ('missing', 'duplicate_worker', 'duplicate_sequence', 'wrong_subject',
                         'wrong_time', 'stale', 'future', 'unshifted', 'wrong_reported_offset',
                         'wrong_payload_worker', 'wrong_payload_time', 'invented_sequence'):
            with self.subTest(mutation=mutation):
                p = self.fixture(); s = p['samples'][0]; m = p['messages'][0]
                if mutation == 'missing': p['samples'].pop()
                elif mutation == 'duplicate_worker': p['samples'][1]['worker'] = s['worker']
                elif mutation == 'duplicate_sequence': p['samples'][1]['sequence'] = 1
                elif mutation == 'wrong_subject': m['subject'] = 'matrix.clock.someone-else'
                elif mutation == 'wrong_time': m['time'] = '2026-10-03T12:00:01Z'
                elif mutation == 'stale': p['observed'] = '2026-10-03T12:00:05Z'
                elif mutation == 'future': p['observed'] = '2026-10-03T11:59:58Z'
                elif mutation == 'unshifted': s['worker_at'] = s['server_at']; s['offset_ns'] = 0
                elif mutation == 'wrong_reported_offset': s['offset_ns'] -= 1
                else:
                    payload = json.loads(base64.b64decode(m['data']))
                    if mutation == 'wrong_payload_worker': payload['worker'] = 'someone-else'
                    elif mutation == 'wrong_payload_time': payload['worker_at'] = s['server_at']
                    else: payload['sequence'] = 100
                    m['data'] = base64.b64encode(json.dumps(payload).encode()).decode()
                with self.assertRaises(ValueError): c.check_probe(p)

    def test_diagnostic_correction_preserves_originals_and_every_other_field(self):
        raw = [dict(Worker=w, At=c.corrected_timestamp('2026-10-03T12:00:00.123456789Z', -o),
                    Epoch=7, Reason='lease_cleanup_lost', Delivery=2, Duration=987)
               for w, o in c.OFFSETS.items()]
        original = copy.deepcopy(raw)
        normalized = c.normalize_records(raw)
        self.assertEqual(raw, original)
        self.assertEqual(c.check_normalized_records(raw, normalized), 5)
        for event in normalized:
            self.assertEqual(event['At'], '2026-10-03T12:00:00.123456789Z')
        for mutation in ('uncorrected', 'changed_epoch', 'rounded', 'dropped'):
            with self.subTest(mutation=mutation):
                altered = copy.deepcopy(normalized)
                if mutation == 'uncorrected': altered = copy.deepcopy(raw)
                elif mutation == 'changed_epoch': altered[0]['Epoch'] = 8
                elif mutation == 'rounded': altered[0]['At'] = '2026-10-03T12:00:00.123456Z'
                else: altered.pop()
                with self.assertRaises(ValueError): c.check_normalized_records(raw, altered)

    def test_negative_offsets_cross_day_boundary_and_preserve_nanoseconds(self):
        self.assertEqual(c.corrected_timestamp('2026-10-03T00:00:01.000000001Z', 5*c.SECOND),
                         '2026-10-02T23:59:56.000000001Z')
        self.assertEqual(c.corrected_timestamp('2026-10-02T23:59:59.999999999Z', -5*c.SECOND),
                         '2026-10-03T00:00:04.999999999Z')
        self.assertEqual(c.corrected_timestamp('2026-10-03T01:00:00+01:00', 0),
                         '2026-10-03T00:00:00Z')
        for value in ('2026-10-03T12:00:00', '2026-10-03T12:00:00.1234567891Z'):
            with self.assertRaises(ValueError): c.corrected_timestamp(value, 0)
        with self.assertRaises(ValueError): c.normalize_records([dict(Worker='foreign', At='2026-10-03T12:00:00Z')])
