import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('copied_review',Path(__file__).with_name('review-tier2-copied-audit.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)

class CopiedAuditReview(unittest.TestCase):
    def fixture(self):
        expected=dict(Invocations=3136,Journals=3136,Entries=34530,Terminal=3136)
        state=dict(messages=0,consumer_count=64)
        result=dict(report=expected.copy(),audit_ns=500_000_000,whole_review_ns=600_000_000,
                    drained_partition_consumers=64,queue_all_three_peers=[dict(state=state.copy()) for _ in range(3)],
                    physical_queue_peers=[dict(node=n,started='2026-10-06T03:00:00Z',finished='2026-10-06T03:00:01Z',
                        state=dict(server_id='server'+str(n),account_details=[dict(stream_detail=[dict(name='WF_RUN',state=state.copy())])])) for n in range(3)])
        return result,expected
    def test_legacy_api_evidence_is_explicitly_not_physical(self):
        result,expected=self.fixture();del result['physical_queue_peers']
        r.review_counts(result,expected)
        self.assertFalse(r.review_physical_peers(result))
        with self.assertRaises(ValueError):r.review_physical_peers(result,True)
    def test_complete_new_evidence_passes(self):
        result,expected=self.fixture();r.review_counts(result,expected)
        self.assertTrue(r.review_physical_peers(result,True))
    def test_cohort_budget_and_api_corruption(self):
        for mutation in ('partial','bool_count','invalid_expected','audit_timeout','whole_timeout','zero_audit','durables','api_missing','api_nonzero','api_bool'):
            with self.subTest(mutation=mutation):
                result,expected=self.fixture()
                if mutation=='partial':result['report']['Terminal']-=1
                elif mutation=='bool_count':result['report']['Invocations']=True
                elif mutation=='invalid_expected':expected['Entries']=1
                elif mutation=='audit_timeout':result['audit_ns']=20_000_000_000
                elif mutation=='whole_timeout':result['whole_review_ns']=20_000_000_000
                elif mutation=='zero_audit':result['audit_ns']=0
                elif mutation=='durables':result['drained_partition_consumers']=63
                elif mutation=='api_missing':result['queue_all_three_peers'].pop()
                elif mutation=='api_nonzero':result['queue_all_three_peers'][0]['state']['messages']=1
                elif mutation=='api_bool':result['queue_all_three_peers'][0]['state']['messages']=False
                with self.assertRaises(ValueError):r.review_counts(result,expected)
    def test_local_physical_corruption(self):
        for mutation in ('missing','duplicate_node','bool_node','duplicate_id','empty_id','error','reversed','nano_reversed','naive','missing_stream','duplicate_stream','messages','bool_messages','missing_messages','durables'):
            with self.subTest(mutation=mutation):
                result,_=self.fixture();peers=result['physical_queue_peers'];peer=peers[0];stream=peer['state']['account_details'][0]['stream_detail']
                if mutation=='missing':peers.pop()
                elif mutation=='duplicate_node':peers[1]['node']=0
                elif mutation=='bool_node':peer['node']=False
                elif mutation=='duplicate_id':peers[1]['state']['server_id']=peer['state']['server_id']
                elif mutation=='empty_id':peer['state']['server_id']=''
                elif mutation=='error':peer['error']='unavailable'
                elif mutation=='reversed':peer['finished']='2026-10-06T02:59:59Z'
                elif mutation=='nano_reversed':peer.update(started='2026-10-06T03:00:00.000000002Z',finished='2026-10-06T03:00:00.000000001Z')
                elif mutation=='naive':peer['started']='2026-10-06T03:00:00'
                elif mutation=='missing_stream':stream.clear()
                elif mutation=='duplicate_stream':stream.append(copy.deepcopy(stream[0]))
                elif mutation=='messages':stream[0]['state']['messages']=1
                elif mutation=='bool_messages':stream[0]['state']['messages']=False
                elif mutation=='missing_messages':del stream[0]['state']['messages']
                elif mutation=='durables':stream[0]['state']['consumer_count']=63
                with self.assertRaises(ValueError):r.review_physical_peers(result,True)

if __name__=='__main__':unittest.main()
