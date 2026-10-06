import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('local_partition_review', Path(__file__).with_name('review-local-tier2-partition.py'))
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def state():
    return dict(schema='js-wf-local-tier2-partition-campaign-v1', source='a'*40,
                row='partition', test=review.TEST, duration='10m', sdk_timeout='18m', race=False,
                seeds=list(range(1,201)), status='native_execution_completed', exit_code=0,
                native_coverage_complete=True, producer_pid=123, producer_start_ticks='456',
                records=[dict(seed=n, root=f'seed-{n:03d}', exit_code=0) for n in range(1,201)])


class LocalPartitionReviewControls(unittest.TestCase):
    def test_full_coverage_is_strict_and_partial_cannot_be_full(self):
        with patch.object(review,'closed_instance'):
            self.assertEqual(len(review.contract(state())),200)
            partial=state();partial.update(status='running',native_coverage_complete=False,records=state()['records'][:1])
            self.assertEqual(review.contract(partial,1)[0]['seed'],1)
            with self.assertRaises(ValueError):review.contract(partial)
            for key,value in [('row','journal'),('duration','35s'),('sdk_timeout','20m'),('race',True),
                              ('source','a'*39),('status','running'),('exit_code',1),('exit_code',False),
                              ('native_coverage_complete',False),('seeds',list(range(1,200)))]:
                changed=state();changed[key]=value
                with self.subTest(key=key,value=value),self.assertRaises(ValueError):review.contract(changed)
            for mutation in ('duplicate','failure','missing','boolean','root'):
                changed=state()
                if mutation=='duplicate':changed['records'][1]['seed']=1
                elif mutation=='failure':changed['records'][17]['exit_code']=1
                elif mutation=='missing':changed['records'].pop()
                elif mutation=='boolean':changed['records'][0]['seed']=True
                else:changed['records'][0]['root']='../other-root'
                with self.subTest(mutation=mutation),self.assertRaises(ValueError):review.contract(changed)

    def test_native_failure_or_substitution_rejects_before_reading_payload(self):
        base=dict(source='a'*40,row='partition',test=review.TEST,duration='10m',race=False,
                  sustained_ten_minutes=True,status='passed',exit_code=0,server_observer_errors=0)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);seed=root/'seed-001';seed.mkdir()
            import json
            for key,value in [('source','b'*40),('row','journal'),('test','Other'),('duration','35s'),
                              ('race',True),('sustained_ten_minutes',False),('status','running'),
                              ('status','failed'),('exit_code',1),('server_observer_errors',1)]:
                changed=copy.deepcopy(base);changed[key]=value
                (seed/'execution.json').write_text(json.dumps(changed))
                with self.subTest(key=key,value=value),self.assertRaises(ValueError):
                    review.seed_review(root,dict(seed=1,root='seed-001'),'a'*40,root,root)


if __name__=='__main__':unittest.main()
