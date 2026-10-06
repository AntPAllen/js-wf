import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('fanout_review', Path(__file__).with_name('review-tier2-retained-fanout.py'))
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)

class FanoutCutReview(unittest.TestCase):
    def fixture(self):
        stamp = lambda n: f'2026-10-06T00:00:0{n}Z'
        ids = [f'child-{i}' for i in range(6)]
        prefix = [dict(kind='StepRequested', payload=dict(kind='call_async', child_id=x), sequence=i+1)
                  for i, x in enumerate(ids)] + [dict(kind='Suspended', sequence=7)]
        fault = dict(scheduled=stamp(0), killed=stamp(2), healed=stamp(3), node=-1, nodes=[0,1,2],
                     fanout_parent='parent', fanout_tail=7, fanout_children=ids, fanout_pending_children=ids)
        cut = dict(ObservedAt=stamp(1), Fault=copy.deepcopy(fault), ParentPrefix=prefix,
                   ChildPrefixes={x: [] for x in ids})
        final = dict(Parent=prefix+[dict(kind='Completed')], Children={}, Grandchildren={})
        for child in ids:
            grandchildren = [child+'-g0', child+'-g1']
            final['Children'][child] = [dict(kind='StepRequested', payload=dict(kind='call_async', child_id=x))
                                       for x in grandchildren]+[dict(kind='Completed')]
            final['Grandchildren'].update({x:[dict(kind='Completed')] for x in grandchildren})
        return fault, cut, copy.deepcopy(prefix), final

    def test_accepts_exact_unfinished_cut_prefix_and_descendants(self):
        r.review_cut(*self.fixture())

    def test_rejects_corrupted_cut_or_final_descendants(self):
        mutations = ('error','nodes','late_cut','early_heal','wrong_cut_parent','tail','not_suspended',
                     'duplicate_child','unrelated_pending','terminal_pending','missing_child_prefix',
                     'recovered_prefix','final_prefix','child_prefix','wrong_final_child',
                     'duplicate_terminal','missing_grandchild','failed_grandchild','shared_grandchild')
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                f,c,recovered,end = self.fixture()
                if mutation == 'error': f['error'] = 'fault failed'
                elif mutation == 'nodes': f['nodes'] = [0,1]
                elif mutation == 'late_cut': c['ObservedAt'] = '2026-10-06T00:00:02.000000001Z'
                elif mutation == 'early_heal': f['healed'] = '2026-10-06T00:00:01.999999999Z'
                elif mutation == 'wrong_cut_parent': c['Fault']['fanout_parent'] = 'different'
                elif mutation == 'tail': f['fanout_tail'] = c['Fault']['fanout_tail'] = 8
                elif mutation == 'not_suspended': c['ParentPrefix'][-1]['kind'] = 'Completed'
                elif mutation == 'duplicate_child': f['fanout_children'][1] = f['fanout_children'][0]; c['Fault'] = copy.deepcopy(f)
                elif mutation == 'unrelated_pending': f['fanout_pending_children'] = ['unrelated']; c['Fault'] = copy.deepcopy(f)
                elif mutation == 'terminal_pending': c['ChildPrefixes']['child-0'] = [dict(kind='Completed')]
                elif mutation == 'missing_child_prefix': del c['ChildPrefixes']['child-0']
                elif mutation == 'recovered_prefix': recovered[0]['payload']['child_id'] = 'changed'
                elif mutation == 'final_prefix': end['Parent'] = copy.deepcopy(end['Parent']); end['Parent'][0]['payload']['child_id'] = 'changed'
                elif mutation == 'child_prefix': c['ChildPrefixes']['child-0'] = [dict(kind='Started')]
                elif mutation == 'wrong_final_child': end['Children']['wrong'] = end['Children'].pop('child-0')
                elif mutation == 'duplicate_terminal': end['Children']['child-0'].append(dict(kind='Completed'))
                elif mutation == 'missing_grandchild': del end['Grandchildren']['child-0-g0']
                elif mutation == 'failed_grandchild': end['Grandchildren']['child-0-g0'][-1]['kind'] = 'Failed'
                elif mutation == 'shared_grandchild': end['Children']['child-1'][0]['payload']['child_id'] = 'child-0-g0'
                with self.assertRaises(ValueError): r.review_cut(f,c,recovered,end)

if __name__ == '__main__': unittest.main()
