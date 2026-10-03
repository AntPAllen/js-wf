import copy
import importlib.util
from pathlib import Path
import re
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

    def test_initial_release_and_cleanup_losses_have_distinct_explanations(self):
        for reason, text in [('lease_release_lost', 'initial lease release'),
                             ('lease_cleanup_lost', 'Lease cleanup')]:
            m,f,r = self.fixture()
            f[0]['Reason'] = reason
            result = explain.check(m,f,r)
            self.assertIn(text, result['explanations'][0]['explanation'])
            self.assertEqual(result['fencing_per_worker'], {'tier3-mixed-0': 1})
            m[0]['fencing_events'] = 0
            with self.assertRaises(ValueError): explain.check(m,f,r)

    def test_every_current_runtime_fencing_reason_has_an_explanation(self):
        source = (Path(__file__).parents[1] / 'worker' / 'worker.go').read_text()
        reasons = set(re.findall(r'"((?:lease_[a-z_]+_lost)|journal_stale)"', source))
        self.assertEqual(reasons, set(explain.FENCING))

    def test_suspended_requires_tail_and_window_and_dry_run_never_publishes(self):
        m,f,_ = self.fixture()
        event=dict(at='2026-10-01T12:00:00Z',kind='suspended',type='parent',id='p',reason='timer',journal_sequence=20,retry_window=100,outcome='acknowledged')
        result=explain.check(m,f,[event]);self.assertIn('timer wait was due',result['explanations'][1]['explanation'])
        for field in ('journal_sequence','retry_window'):
            altered=copy.deepcopy(event);altered[field]=0
            with self.assertRaises(ValueError):explain.check(m,f,[altered])
        event['outcome']='dry_run';event['retry_window']=0
        self.assertIn('no publication was attempted',explain.check(m,f,[event])['explanations'][1]['explanation'])


    def test_timer_source_due_time_and_step_evidence(self):
        m,f,_=self.fixture()
        for kind,reasons in [('timer',['timer','timer_start','timer_await']),('fallback-timer',['due_fallback_timer'])]:
            for reason in reasons:
                e=dict(at='2026-10-01T12:00:00Z',kind=kind,type='timer',id='t',reason=reason,source_sequence=10,invocation_sequence=4,journal_sequence=20,fire_at='2026-10-01T11:59:59Z',timer_step=0,outcome='acknowledged')
                self.assertEqual(explain.check(m,f,[e])['repair_records'],1)
                for field,value in [('fire_at',''),('fire_at','2026-10-01T11:59:59'),('source_sequence',0),('invocation_sequence',0)]+([('journal_sequence',0)] if kind=='timer' else [('timer_step',None),('timer_step',-1),('timer_step',True)]):
                    altered=copy.deepcopy(e);altered[field]=value
                    with self.subTest(kind=kind,field=field,value=value),self.assertRaises(ValueError):explain.check(m,f,[altered])
                e['outcome']='uncertain';e['error']='committed publication acknowledgement lost'
                self.assertIn('may have committed',explain.check(m,f,[e])['explanations'][1]['explanation'])
                e['outcome']='dry_run';e.pop('error')
                self.assertIn('no publication was attempted',explain.check(m,f,[e])['explanations'][1]['explanation'])


class ProcessCounterExplanations(unittest.TestCase):
    def test_retired_counters_stay_unknown_and_survivor_counters_are_checked(self):
        _,f,r=EventExplanations().fixture()
        f[0]['Worker']='retired'
        expected={'retired':None,'survivor':0}
        result=explain.check(None,f,r,expected_workers=expected)
        self.assertFalse(result['counter_cross_checks_complete'])
        self.assertFalse(result['clears_full_tier3_release'])
        self.assertIn('may have committed',result['explanations'][1]['explanation'])
        with self.assertRaises(ValueError):explain.check(None,f,r,expected_workers={'retired':0,'survivor':0})
        f[0]['Worker']='survivor'
        with self.assertRaises(ValueError):explain.check(None,f,r,expected_workers=expected)
        f[0]['Worker']='unknown'
        with self.assertRaises(ValueError):explain.check(None,f,r,expected_workers=expected)
