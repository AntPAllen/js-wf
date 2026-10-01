import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('explain', Path(__file__).with_name('explain-tier3-events.py'))
explain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(explain)

class EventExplanations(unittest.TestCase):
    def fixture(self):
        metrics = [{'fencing_events': 1}] + [{'fencing_events': 0} for _ in range(4)]
        fences = [dict(At='2026-10-01T12:00:00Z', Worker='tier3-mixed-0', Type='child', ID='c', RunSequence=3, Delivery=2, Epoch=7, Reason='lease_heartbeat_lost', Error='lease was lost')]
        repairs = [dict(at='2026-10-01T12:00:00Z', kind='signal', type='parent', id='p', reason='unconsumed_signal_nonterminal_generation', source_sequence=10, invocation_sequence=4, outcome='uncertain', error='timeout')]
        return metrics, fences, repairs

    def test_explains_every_record_without_claiming_server_cause_or_soak(self):
        result = explain.check(*self.fixture())
        self.assertEqual((result['fencing_records'], result['repair_records']), (1, 1))
        self.assertEqual(len(result['explanations']), 2)
        self.assertFalse(result['clears_full_tier3_release'])
        self.assertFalse(result['server_root_causes_confirmed'])
        self.assertIn('may have committed', result['explanations'][1]['explanation'])

    def test_rejects_missing_or_misrepresented_evidence(self):
        cases = []
        m, f, r = self.fixture(); f.clear(); cases.append((m,f,r))
        for field, value in [('Reason','server_bug'), ('Worker','unknown'), ('Epoch',0), ('RunSequence',0), ('Error',''), ('At','2026-10-01T12:00:00')]:
            m,f,r = self.fixture(); f[0][field]=value; cases.append((m,f,r))
        for field,value in [('outcome','acknowledged'), ('error',''), ('source_sequence',0), ('invocation_sequence',0), ('reason','lost_signal')]:
            m,f,r = self.fixture(); r[0][field]=value; cases.append((m,f,r))
        for data in cases:
            with self.subTest(data=data), self.assertRaises(ValueError): explain.check(*data)

    def test_suspended_requires_tail_and_window_and_dry_run_never_publishes(self):
        m,f,_ = self.fixture()
        event=dict(at='2026-10-01T12:00:00Z',kind='suspended',type='parent',id='p',reason='timer',journal_sequence=20,retry_window=100,outcome='acknowledged')
        result=explain.check(m,f,[event]);self.assertIn('timer wait was due',result['explanations'][1]['explanation'])
        for field in ('journal_sequence','retry_window'):
            altered=copy.deepcopy(event);altered[field]=0
            with self.assertRaises(ValueError):explain.check(m,f,[altered])
        event['outcome']='dry_run';event['retry_window']=0
        self.assertIn('no publication was attempted',explain.check(m,f,[event])['explanations'][1]['explanation'])
