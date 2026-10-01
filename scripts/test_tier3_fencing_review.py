import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('review',Path(__file__).with_name('review-tier3-fencing.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)

class FencingTimelineReview(unittest.TestCase):
    def fixture(self,fetch='02',terminal='03',fence='04'):
        stamp=lambda second:'2026-10-01T12:00:'+second+'Z'
        event=dict(At=stamp(fence),Worker='owner',Type='short',ID='x',RunSequence=1,Delivery=1,Epoch=7)
        fetched=dict(event,At=stamp(fetch),Stage='fetched')
        ack=dict(event,At=stamp('05'),Stage='ack',Worker='successor',RunSequence=2)
        terminal=dict(type='short',id='x',event='terminal',observed=stamp(terminal))
        return [event],[fetched,ack],[terminal],[dict(killed=stamp('01'),healed=stamp('06'))]

    def test_terminal_before_fencing_does_not_imply_duplicate_fetch(self):
        for fetch,terminal,label in [('02','01','already_terminal_before_fetch'),('02','03','completed_during_original_delivery'),('02','05','completed_after_fencing')]:
            result=r.review(*self.fixture(fetch,terminal))
            self.assertEqual(result['records'][0]['classification'],label)
            self.assertEqual(len(result['records'][0]['later_invocation_ack_observations']),1)
            self.assertEqual(result['records'][0]['later_exact_delivery_ack_observations'],[])
            self.assertFalse(result['ack_observations_prove_broker_commit'])
            self.assertFalse(result['clears_full_tier3_release'])

    def test_missing_or_wrong_delivery_is_rejected(self):
        for mutation in ('missing','wrong_worker','wrong_sequence','duplicate','reversed','missing_terminal'):
            with self.subTest(mutation=mutation):
                f,d,l,fl=self.fixture()
                if mutation=='missing':d.pop(0)
                elif mutation=='wrong_worker':d[0]['Worker']='other'
                elif mutation=='wrong_sequence':d[0]['RunSequence']=9
                elif mutation=='duplicate':d.append(copy.deepcopy(d[0]))
                elif mutation=='reversed':d[0]['At']='2026-10-01T12:00:06Z'
                elif mutation=='missing_terminal':l.clear()
                with self.assertRaises(ValueError):r.review(f,d,l,fl)

    def test_pause_requires_matching_original_epoch_and_terminal_inside_stop(self):
        f,d,l,fl=self.fixture()
        fl[0].update(worker='owner',paused='2026-10-01T12:00:02Z',resumed='2026-10-01T12:00:04Z',paused_leases=[dict(key='short.x',epoch=7)])
        self.assertEqual(r.review(f,d,l,fl)['records'][0]['classification'],'completed_while_original_owner_stopped')
        fl[0]['paused_leases'][0]['epoch']=8
        self.assertEqual(r.review(f,d,l,fl)['records'][0]['classification'],'completed_during_original_delivery')

    def test_nanosecond_order_and_outside_faults_remain_visible(self):
        f,d,l,fl=self.fixture()
        d[0]['At']='2026-10-01T12:00:02.000000001Z'
        l[0]['observed']='2026-10-01T12:00:02.000000002Z'
        fl[0]['healed']='2026-10-01T12:00:03Z'
        result=r.review(f,d,l,fl)
        self.assertEqual(result['outside_confirmed_faults'],1)
        self.assertEqual(result['records'][0]['classification'],'completed_during_original_delivery')

    def test_controller_terminal_windows_preserve_uncertain_ordering(self):
        for lower,label in [('01','completion_window_overlaps_fetch'),('03','completion_window_overlaps_fencing'),('04.5','completed_after_fencing')]:
            f,d,l,fl=self.fixture(terminal='05')
            l[0]['observed_lower']='2026-10-01T12:00:'+lower+'Z'
            self.assertEqual(r.review(f,d,l,fl)['records'][0]['classification'],label)
        f,d,l,fl=self.fixture()
        l[0]['observed_lower']='2026-10-01T12:00:06Z'
        with self.assertRaises(ValueError):r.review(f,d,l,fl)
